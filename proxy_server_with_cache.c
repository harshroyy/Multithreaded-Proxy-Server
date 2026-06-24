#include "proxy_parse.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <netdb.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/wait.h>
#include <time.h>
#include <pthread.h>
#include <semaphore.h>


#define MAX_CLIENTS 10
#define MAX_BYTES 4096
#define MAX_ELEMENT_SIZE (10 * (1 << 10))
#define MAX_SIZE (200 * (1 << 20)) // cache size in bytes

typedef struct cache_element cache_element; // we can write cache_element instead of struct cache_element

struct cache_element {
    char* data;
    int len;
    char* url;
    time_t lru_time_track;
    cache_element* next;
};


cache_element* find(char* url);
int add_cache_element(char* data, int size, char* url);
void remove_cache_element();

int port_number = 8080;
int proxy_socketId;

// Hume sockets create karne honge kyunki yeh Express use nahi kar raha hai.
// Pehle ek port par listen karenge aur clients ki connections accept karenge.
//
// Jab client request bhejega, hum us request ko read aur parse karenge.
// Phir check karenge ki requested data cache me hai ya nahi.
//
// Agar cache me mil gaya, toh direct cached response client ko bhej denge.
// Agar cache me nahi mila, toh request actual server ko forward karenge,
// server se response lenge, client ko bhejenge, aur future use ke liye
// us response ko cache me store kar denge.

pthread_t tid[MAX_CLIENTS]; // Array of thread IDs for handling multiple clients
sem_t semaphore; // Semaphore to limit the number of concurrent clients
pthread_mutex_t lock; // Mutex for synchronizing access to the cache

cache_element* head;
int cache_size;

// Function to find an element in the cache
cache_element* find(char* url) {
    cache_element* site = NULL;
    int time_lock_val = pthread_mutex_lock(&lock); // lock lagao
    printf("Remove cache lock acquired %d\n", time_lock_val);
    if(head != NULL) {
        site = head; // start from head
        while(site != NULL) { // traverse cache
            if(!strcmp(site -> url , url)) { // Match mil gaya 
                printf("LRU time track before : %ld\n", site -> lru_time_track);
                printf("\n url found\n");
                // Cache full hone par remove karna hai: Sabse purana page
                site -> lru_time_track = time(NULL); // Update LRU Time
                printf("LRU time track after : %ld\n", site -> lru_time_track);
                break;
            }
            site = site -> next;
        }
    }
    else {
        printf("url not found\n");
    }
    // Unlock
    int temp_lock_val = pthread_mutex_unlock(&lock);
    printf("Remove cache lock released %d\n", temp_lock_val);
    return site;
}

// Function to remove the least recently used element from the cache
void remove_cache_element() {
    // Note: The caller (add_cache_element) must hold 'lock' before calling this function.
    if (head == NULL) {
        return;
    }

    cache_element* lru = head;
    cache_element* lru_prev = NULL;

    cache_element* curr = head;
    cache_element* curr_prev = NULL;

    // Traverse the cache list to find the Least Recently Used (LRU) element.
    // The LRU element is the one with the oldest (smallest) lru_time_track.
    while (curr != NULL) {
        if (curr->lru_time_track < lru->lru_time_track) {
            lru = curr;
            lru_prev = curr_prev;
        }
        curr_prev = curr;
        curr = curr->next;
    }

    // Unlink the LRU element from the linked list.
    if (lru_prev == NULL) {
        // LRU element is the head of the list.
        head = head->next;
    } else {
        // LRU element is in the middle or end of the list.
        lru_prev->next = lru->next;
    }

    // Calculate memory size of the removed element and update global cache size.
    int element_size = lru->len + 1 + strlen(lru->url) + sizeof(cache_element);
    cache_size -= element_size;

    // Free the dynamically allocated memory of the element to prevent memory leaks.
    free(lru->data);
    free(lru->url);
    free(lru);
}

// Function that stores a new response in the cache.
int add_cache_element(char* data, int size, char* url) {
    int temp_lock_val = pthread_mutex_lock(&lock); // Lock lagao
    printf("add cache lock acquired %d\n", temp_lock_val);

    // Calculate memory required for the new cache element
    int element_size = size + 1 + strlen(url) + sizeof(cache_element);

    if(element_size > MAX_ELEMENT_SIZE) { // Agar element size MAX_ELEMENT_SIZE se zyada hai, toh
        temp_lock_val = pthread_mutex_unlock(&lock);
        printf("add cache lock released %d\n", temp_lock_val);
        return 0; // Cache me store nahi hoga
    }
    else { // Agar element size MAX_ELEMENT_SIZE se kam hai, toh
        while(cache_size + element_size > MAX_SIZE) { // Jab tak cache full hai, LRU page hatao
            remove_cache_element(); // LRU page hatao
        }

        // Create new cache element
        cache_element *element = (cache_element*)malloc(sizeof(cache_element));
        element -> data = (char*) malloc(size+1); // Allocate memory for data
        strcpy(element -> data, data); // Copy data
        element -> url = (char*) malloc(strlen(url)+sizeof(char)); // Allocate memory for URL
        strcpy(element -> url, url); // Copy URL
        element -> lru_time_track = time(NULL); // Set LRU time
        element -> len = size; // Set data length
        element -> next = head; // Link to head
        head = element; // Update head
        cache_size += element_size; // Update cache size
        temp_lock_val = pthread_mutex_unlock(&lock); // Unlock
        printf("add cache lock released %d\n", temp_lock_val);
        return 1; // Return 1 if cache element added successfully
    }
    return 0;
}

// Function to send error messages to the client
int sendErrorMessage(int socket, int status_code) {
    char str[1024];
    char currentTime[50];
    time_t now = time(0);

    struct tm data = *gmtime(&now);
    strftime(currentTime, 50, "%a, %d %b %Y %H:%M:%S GMT", &data);

    switch(status_code) {
        case 400:
            sprintf(str, "HTTP/1.1 400 Bad Request\r\nContent-Type: text/html\r\nConnection: close\r\nDate: %s\r\n\r\n" 
                "<html><head><title>400 Bad Request</title></head><body>" 
                "<h1>400 Bad Request</h1><p>The request was malformed or invalid.</p></body></html>\r\n", 
                currentTime);
            break;
        case 403:
            sprintf(str, "HTTP/1.1 403 Forbidden\r\nContent-Type: text/html\r\nConnection: close\r\nDate: %s\r\n\r\n" 
                "<html><head><title>403 Forbidden</title></head><body>" 
                "<h1>403 Forbidden</h1><p>Access to the requested resource is denied.</p></body></html>\r\n", 
                currentTime);
            break;
        case 404:
            sprintf(str, "HTTP/1.1 404 Not Found\r\nContent-Type: text/html\r\nConnection: close\r\nDate: %s\r\n\r\n" 
                "<html><head><title>404 Not Found</title></head><body>" 
                "<h1>404 Not Found</h1><p>The requested resource could not be found.</p></body></html>\r\n", 
                currentTime);
            break;
        case 500:
            sprintf(str, "HTTP/1.1 500 Internal Server Error\r\nContent-Type: text/html\r\nConnection: close\r\nDate: %s\r\n\r\n" 
                "<html><head><title>500 Internal Server Error</title></head><body>" 
                "<h1>500 Internal Server Error</h1><p>The server encountered an unexpected condition.</p></body></html>\r\n", 
                currentTime);
            break;
        case 502:
            sprintf(str, "HTTP/1.1 502 Bad Gateway\r\nContent-Type: text/html\r\nConnection: close\r\nDate: %s\r\n\r\n" 
                "<html><head><title>502 Bad Gateway</title></head><body>" 
                "<h1>502 Bad Gateway</h1><p>The server received an invalid response from the upstream server.</p></body></html>\r\n", 
                currentTime);
            break;
        case 503:
            sprintf(str, "HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/html\r\nConnection: close\r\nDate: %s\r\n\r\n" 
                "<html><head><title>503 Service Unavailable</title></head><body>" 
                "<h1>503 Service Unavailable</h1><p>The server is temporarily unable to handle the request.</p></body></html>\r\n", 
                currentTime);
            break;
        case 504:
            sprintf(str, "HTTP/1.1 504 Gateway Timeout\r\nContent-Type: text/html\r\nConnection: close\r\nDate: %s\r\n\r\n" 
                "<html><head><title>504 Gateway Timeout</title></head><body>" 
                "<h1>504 Gateway Timeout</h1><p>The server did not receive a timely response from the upstream server.</p></body></html>\r\n", 
                currentTime);
            break;
        default:
            sprintf(str, "HTTP/1.1 500 Internal Server Error\r\nContent-Type: text/html\r\nConnection: close\r\nDate: %s\r\n\r\n" 
                "<html><head><title>500 Internal Server Error</title></head><body>" 
                "<h1>500 Internal Server Error</h1><p>An unexpected condition was encountered.</p></body></html>\r\n", 
                currentTime);
            break;
    }
    send(socket, str, strlen(str), 0);
    return 0;
}
    
// Proxy server se actual remote server (Google/Facebook/etc.) tak connection banana.
// Client | Proxy | connectRemoteServer() | Google Server
int connectRemoteServer(char* host_addr, int port_num) {
    // Step 1: Remote Socket Create
    int remoteSocket = socket(AF_INET, SOCK_STREAM, 0);
    if(remoteSocket < 0) { // error check
        printf("Error creating socket");
        return -1;
    }

    // Step 2: Get Host IP - Computer directly: google.com se connect nahi karta. 
    // Usse IP chahiye. google.com -> 142.250.xxx.xxx 
    struct hostent* host = gethostbyname(host_addr);
    if(host == NULL) { // If DNS Fails
        fprintf(stderr, "no such host exists\n");
        return -1;
    }

    // Step 3: Prepare Server Address
    struct sockaddr_in server_addr; // Remote server ka address store hoga.

    bzero((char *)&server_addr, sizeof(server_addr)); // Sab fields ko 0 kar diya.
    server_addr.sin_family = AF_INET; // IPv4 use krr rhe
    server_addr.sin_port = htons(port_num); // Port Number

    // bcopy copies bytes from one memory location to another.
    bcopy((char*) &host -> h_addr, (char*) &server_addr.sin_addr.s_addr, host -> h_length);

    // Step 4: Connect to Remote Server - TCP 3-way handshake hota hai:
    // 1. SYN , 2. SYN + ACK , 3. ACK - isliye connect function use karte hain.
    if(connect(remoteSocket, (struct sockaddr*)&server_addr, (size_t)sizeof(server_addr)) < 0) {
        printf("Error connecting to server\n");
        return -1;
    }

    return remoteSocket;
}

// Function to handle client requests - Client ---> Proxy ---> Google/Facebook Server
// client_socketId -> client se baat karne ke liye
// request         -> parsed HTTP request
// tempReq         -> original request (cache key)
int handle_request(int client_socketId, ParsedRequest *request, char* tempReq) {
    char *buf = (char *) malloc(sizeof(char)*MAX_BYTES); // Yeh buffer bana for new request
    
    // Request Reconstruct Karna - Eg : path = /index.html version = HTTP/1.1
    // buf = "GET /index.html HTTP/1.1" 
    strcpy(buf, "GET");
    strcat(buf, request -> path);
    strcat(buf, " ");
    strcat(buf, request -> version);
    strcat(buf, "\r\n");
    
    size_t len = strlen(buf);

    // Proxy server remote server ko bol raha hai: Response ke baad connection close kar dena.
    if(ParsedHeader_set(request, "Connection", "close") < 0) {
        printf("Set Header Key is not working\n");
    }

    // Host Header Check - Host header is required for the request to be valid.
    if(ParsedHeader_get(request, "Host") == NULL) {
        if(ParsedHeader_set(request, "Host", request -> host) < 0) {
            printf("Error setting Host header\n");
        }
    } 

    // Headers Dubara Banana - Ab request ke saare headers append honge.
    if(ParsedRequest_unparse_headers(request, buf+len, (size_t)MAX_BYTES - len) < 0) {
        printf("Error unparsing header\n");
    }

    // Port nikalo
    int server_port = 80;
    if(request -> port != NULL) {
        server_port = atoi(request -> port);
    }

    // Remote Server Connect
    int remoteSocketId = connectRemoteServer(request -> host, server_port);
    if(remoteSocketId < 0) {
        sendErrorMessage(client_socketId, 502);
        return -1;
    }   

    // Send request to remote server
    int bytes_send = send(remoteSocketId, buf, strlen(buf), 0);
    bzero(buf, MAX_BYTES);

    // Recieve response from remote server
    bytes_send = recv(remoteSocketId, buf, MAX_BYTES-1, 0);

    // Store response in a temporary buffer
    char* temp_buffer = (char*) malloc(sizeof(char)*MAX_BYTES); // Ye cache ke liye copy banane wala buffer hai.
    int temp_buffer_size = MAX_BYTES;
    int temp_buffer_index = 0;

    // Ye loop tab tak chalega jab tak remote server response bhej raha hai.
    while(bytes_send > 0) {
       // Send the response to the client
       bytes_send = send(client_socketId, buf, bytes_send, 0);
       
        // Copy the response to the temporary buffer for cache
        for(int i = 0; i < bytes_send; i++) {
             temp_buffer[temp_buffer_index] = buf[i];
             temp_buffer_index++;
        }

       // Resize buffer to store more data
       temp_buffer_size += MAX_BYTES;
       temp_buffer = (char*) realloc(temp_buffer, temp_buffer_size);
       if(bytes_send < 0) {
            perror("Error in sending to the client\n");
            break;
       }
       bzero(buf, MAX_BYTES);

       // Receive Next Chunk of Response from Remote Server
       bytes_send = recv(remoteSocketId, buf, MAX_BYTES-1, 0); 
    }

    // Add Null Terminator to the temporary buffer
    temp_buffer[temp_buffer_index] = '\0';
    free(buf);
    // Add to Cache
    add_cache_element(temp_buffer, strlen(temp_buffer), tempReq);
    free(temp_buffer);
    close(remoteSocketId); // Close connection to remote server
    return 0;
}

// Check if the HTTP version is valid
int checkHTTPversion(char* msg) {
    int version = -1;

    // strncmp(string1, string2, num_characters_to_compare)
    if(strncmp(msg, "HTTP/1.1", 8) == 0) {
        version = 1;
    } 
    else if (strncmp(msg, "HTTP/1.0", 8) == 0) {
        version = 1;
    }
    else {
        version = -1; // Invalid HTTP version
    }
    return version;
}       

// Ye function basically har client ke liye ek thread me run hoga
void *thread_fn(void *socketNew) {
    sem_wait(&semaphore); //"Thread permission maang raha hai andar enter karne ki."
    int p;
    sem_getvalue(&semaphore, &p); //Ye batayega ki abhi kitne threads andar hai.
    printf("semaphore value is : %d\n" , p); 
    int *t = (int*) socketNew;
    int socket = *t;
    int bytes_send_client, len;

    // Buffer Banana - Memory allocate kar rahe ho , To 4096 bytes ka buffer ban gaya.
    char *buffer = (char*) calloc(MAX_BYTES, sizeof(char)); // Dynamic memory allocation for buffer
    bzero(buffer, MAX_BYTES);
    // Client se HTTP request read kar rahe ho.
    bytes_send_client = recv(socket, buffer, MAX_BYTES, 0);

    // HTTP Request ko Complete Padhenge. 
    // Problem : HTTP Request header ek baar me nahi aa sakta 4096 bytes me.
    while(bytes_send_client > 0) {
        len = strlen(buffer);
        if(strstr(buffer, "\r\n\r\n") == NULL) {
            bytes_send_client = recv(socket, buffer + len, MAX_BYTES - len, 0);
        }
        else {
            break;
        }
    }   
    
    // ye ek temporary buffer hai jo actual request store karega.
    char *tempReq = (char *) malloc(strlen(buffer) + 1);
    strcpy(tempReq, buffer);

    // Cache Check: Cache me eg. google.com ka response hai kya?
    struct cache_element* temp = find(tempReq);

    if(temp != NULL) { // if cache hit (mil gya)
        int size = temp -> len/sizeof(char);
        int pos = 0;
        char response[MAX_BYTES];
        // Cache se data chunks me client ko bhej rahe hain.
        while(pos < size) {
            bzero(response, MAX_BYTES);
            for(int i=0; i<MAX_BYTES; i++) {
                response[i] = temp -> data[i];
                pos++;
            }   
            send(socket, response, MAX_BYTES, 0);
        } 
        printf("Data send to client successfully\n");
        printf("%s\n\n", response);
    }
    else if(bytes_send_client > 0) { // if cache miss (nahi mila) 
        len = strlen(buffer);  
        // Create a ParsedRequest object to store the parsed request.
        ParsedRequest *request = ParsedRequest_create();
        
        if(ParsedRequest_parse(request, buffer, len) < 0) { // if error in parsing
            printf("Error parsing request\n");
        } 
        else { // if parsing is successful
            bzero(buffer, MAX_BYTES); 
            if(!strcmp(request -> method, "GET")) { // if method is GET
                if(request -> host && request -> path && checkHTTPversion(request -> version) == 1) { // if host and path are present and HTTP version is 1
                    bytes_send_client = handle_request(socket, request, tempReq);
                    if(bytes_send_client == -1) {
                        sendErrorMessage(socket, 500);
                    }
                } 
                else {
                    sendErrorMessage(socket, 400);
                } 
            }
            else {
                printf("This code only handle GET method\n");
                sendErrorMessage(socket, 405);
            } 
        }
        ParsedRequest_destroy(request);
    }    
    else if(bytes_send_client == 0) {
        printf("Client Disconnected\n"); 
    }
    shutdown(socket, SHUT_RDWR); // close connection
    close(socket);
    free(buffer); // free memory
    sem_post(&semaphore); // signal semaphore
    sem_getvalue(&semaphore, &p);
    printf("Semaphore value is : %d\n" , p);
    free(tempReq);
    return NULL;
}

int main(int argc, char* argv[]) {
    int client_socketId;
    int client_len;

    struct sockaddr_in server_addr, client_addr;

    sem_init(&semaphore, 0, MAX_CLIENTS);
    pthread_mutex_init(&lock, NULL);
    
    if(argc == 2) {
        port_number = atoi(argv[1]);
    }
    else {
        printf("Too few arguments\n");
        exit(1);
    }

    printf("Starting proxy server on port %d\n", port_number);

    proxy_socketId = socket(AF_INET, SOCK_STREAM, 0); 
    // Address Family Internet - IPv4 use karunga.
    // SOCK_STREAM - TCP connection ke liye use hota hai.

    if(proxy_socketId < 0) {
        printf("Error creating socket\n");
        exit(1);
    }

    int reuse = 1; // Reuse ka value 1 set kar rahe hain, taaki agar socket close ho jaye toh turant reuse ho sake.
    if(setsockopt(
         proxy_socketId, // socket ID
         SOL_SOCKET, // Socket-level option modify kar rahe hain.
         SO_REUSEADDR, // Option ka naam.
         (const char*)&reuse, // Option ka value. Is case me, reuse ka value 1 hai.
         sizeof(reuse)) < 0
        ) {
        printf("Error setting socket options\n");
        exit(1);
    }

    bzero((char*)&server_addr, sizeof(server_addr)); // C mein sbb garbage value ko zero karne ke liye bzero use hota hai. 
    // Server address ko zero kar rahe hain.

    // Ye ek structure hai jo server ka address store karta hai.
    server_addr.sin_family = AF_INET; // Address Family Internet - IPv4 use kar rahe hain.
    server_addr.sin_port = htons(port_number); // Host TO Network Short - Port number ko network byte order me convert kar rahe hain.
    server_addr.sin_addr.s_addr = INADDR_ANY; // INADDR_ANY ka matlab hai ki server kisi bhi IP address se connection accept karega.

    // bind - socket humne bnaaya but wo khaali h , for assigning address to the socket, we use bind.
    if(bind(proxy_socketId, (struct sockaddr*) &server_addr, sizeof(server_addr)) < 0) {
        printf("Error binding socket\n");
        exit(1);
    }

    printf("Binding on port %d.\n", port_number);

    // Listen kar rahe hain, aur maximum 10 clients ko queue me rakh sakte hain.
    int listen_status = listen(proxy_socketId, MAX_CLIENTS);
    if(listen_status < 0) {
        printf("Error listening on socket\n");
        exit(1);
    }

    int i = 0;
    int Connected_socketId[MAX_CLIENTS]; // Array to store connected client socket IDs
    
    while(1) { // inf loop - ki server hamesha clients ke liye ready rahe.
        bzero((char*) &client_addr, sizeof(client_addr)); // Purani client ki information hata rahe hain.
        
        client_len = sizeof(client_addr); 
        // Accept kar rahe hain client connection ko.
        client_socketId = accept(proxy_socketId, (struct sockaddr*) &client_addr, (socklen_t*)&client_len); 
        
        if(client_socketId < 0) {
            printf("Error accepting client connection\n");
            continue; // Continue to accept next client
        }
        else {
            Connected_socketId[i] = client_socketId; // Store the connected client socket ID
        }

        // Get the client's IP address and print it
        struct sockaddr_in *client_ptr = (struct sockaddr_in*) &client_addr;   
        struct in_addr ip_addr = client_ptr -> sin_addr; // Client ka IP address nikal rahe hain.
        char str[INET_ADDRSTRLEN]; // INET_ADDRSTRLEN - IPv4 address ke liye string length define karta hai.
        inet_ntop(AF_INET, &ip_addr, str, INET_ADDRSTRLEN); // IP address ko string me convert kar rahe hain.
        printf("Client is connected with port number %d and IP address is %s\n", ntohs(client_ptr->sin_port), str); // Client ka IP address print kar
        
        // Thread create kar rahe hain client ko handle karne ke liye.
        pthread_create(&tid[i], NULL, thread_fn, (void *)&Connected_socketId[i]); 
        i++;
    }

    close(proxy_socketId);
    return 0;
} 

 