#include "proxy_parse.h"
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define MAX_CLIENTS 10
#define MAX_BYTES 4096
#define MAX_ELEMENT_SIZE (10 * (1 << 10))
#define MAX_SIZE (200 * (1 << 20)) // cache size in bytes

typedef struct cache_element cache_element;

struct cache_element {
  char *data;
  int len;
  char *url;
  time_t lru_time_track;
  cache_element *next;
};

cache_element *find(char *url);
int add_cache_element(char *data, int size, char *url);
void remove_cache_element();

int port_number = 8080;
int proxy_socketId;

pthread_t tid[MAX_CLIENTS];
sem_t semaphore;
pthread_mutex_t lock;

cache_element *head;
int cache_size;

// Function to find an element in the cache
cache_element *find(char *url) {
  cache_element *site = NULL;
  int time_lock_val = pthread_mutex_lock(&lock);
  printf("Remove cache lock acquired %d\n", time_lock_val);
  if (head != NULL) {
    site = head;
    while (site != NULL) {
      if (!strcmp(site->url, url)) {
        printf("LRU time track before : %ld\n", site->lru_time_track);
        printf("\n url found\n");
        site->lru_time_track = time(NULL);
        printf("LRU time track after : %ld\n", site->lru_time_track);
        break;
      }
      site = site->next;
    }
    // BUG FIX #1: moved "url not found" message to correct branch
    if (site == NULL) {
      printf("url not found\n");
    }
  } else {
    printf("Cache is empty\n");
  }
  int temp_lock_val = pthread_mutex_unlock(&lock);
  printf("Remove cache lock released %d\n", temp_lock_val);
  return site;
}

// Function to remove the least recently used element from the cache
// Note: caller (add_cache_element) must hold 'lock' before calling this
// function.
void remove_cache_element() {
  if (head == NULL) {
    return;
  }

  cache_element *lru = head;
  cache_element *lru_prev = NULL;

  cache_element *curr = head;
  cache_element *curr_prev = NULL;

  while (curr != NULL) {
    if (curr->lru_time_track < lru->lru_time_track) {
      lru = curr;
      lru_prev = curr_prev;
    }
    curr_prev = curr;
    curr = curr->next;
  }

  if (lru_prev == NULL) {
    head = head->next;
  } else {
    lru_prev->next = lru->next;
  }

  int element_size = lru->len + 1 + strlen(lru->url) + sizeof(cache_element);
  cache_size -= element_size;

  free(lru->data);
  free(lru->url);
  free(lru);
}

// Function that stores a new response in the cache.
int add_cache_element(char *data, int size, char *url) {
  int temp_lock_val = pthread_mutex_lock(&lock);
  printf("add cache lock acquired %d\n", temp_lock_val);

  int element_size = size + 1 + strlen(url) + sizeof(cache_element);

  if (element_size > MAX_ELEMENT_SIZE) {
    temp_lock_val = pthread_mutex_unlock(&lock);
    printf("add cache lock released %d\n", temp_lock_val);
    return 0;
  } else {
    while (cache_size + element_size > MAX_SIZE) {
      remove_cache_element();
    }

    cache_element *element = (cache_element *)malloc(sizeof(cache_element));
    element->data = (char *)malloc(size + 1);
    memcpy(element->data, data, size); // BUG FIX #5 (partial): use memcpy not
                                       // strcpy so binary data is safe
    element->data[size] = '\0';
    element->url = (char *)malloc(strlen(url) + 1);
    strcpy(element->url, url);
    element->lru_time_track = time(NULL);
    element->len = size;
    element->next = head;
    head = element;
    cache_size += element_size;
    temp_lock_val = pthread_mutex_unlock(&lock);
    printf("add cache lock released %d\n", temp_lock_val);
    return 1;
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

  switch (status_code) {
  case 400:
    sprintf(str,
            "HTTP/1.1 400 Bad Request\r\nContent-Type: "
            "text/html\r\nConnection: close\r\nDate: %s\r\n\r\n"
            "<html><head><title>400 Bad Request</title></head><body>"
            "<h1>400 Bad Request</h1><p>The request was malformed or "
            "invalid.</p></body></html>\r\n",
            currentTime);
    break;
  case 403:
    sprintf(str,
            "HTTP/1.1 403 Forbidden\r\nContent-Type: text/html\r\nConnection: "
            "close\r\nDate: %s\r\n\r\n"
            "<html><head><title>403 Forbidden</title></head><body>"
            "<h1>403 Forbidden</h1><p>Access to the requested resource is "
            "denied.</p></body></html>\r\n",
            currentTime);
    break;
  case 404:
    sprintf(str,
            "HTTP/1.1 404 Not Found\r\nContent-Type: text/html\r\nConnection: "
            "close\r\nDate: %s\r\n\r\n"
            "<html><head><title>404 Not Found</title></head><body>"
            "<h1>404 Not Found</h1><p>The requested resource could not be "
            "found.</p></body></html>\r\n",
            currentTime);
    break;
  case 405:
    // BUG FIX #7: added missing 405 Method Not Allowed case
    sprintf(str,
            "HTTP/1.1 405 Method Not Allowed\r\nContent-Type: "
            "text/html\r\nConnection: close\r\nDate: %s\r\n\r\n"
            "<html><head><title>405 Method Not Allowed</title></head><body>"
            "<h1>405 Method Not Allowed</h1><p>Only GET method is "
            "supported.</p></body></html>\r\n",
            currentTime);
    break;
  case 500:
    sprintf(str,
            "HTTP/1.1 500 Internal Server Error\r\nContent-Type: "
            "text/html\r\nConnection: close\r\nDate: %s\r\n\r\n"
            "<html><head><title>500 Internal Server Error</title></head><body>"
            "<h1>500 Internal Server Error</h1><p>The server encountered an "
            "unexpected condition.</p></body></html>\r\n",
            currentTime);
    break;
  case 502:
    sprintf(str,
            "HTTP/1.1 502 Bad Gateway\r\nContent-Type: "
            "text/html\r\nConnection: close\r\nDate: %s\r\n\r\n"
            "<html><head><title>502 Bad Gateway</title></head><body>"
            "<h1>502 Bad Gateway</h1><p>The server received an invalid "
            "response from the upstream server.</p></body></html>\r\n",
            currentTime);
    break;
  case 503:
    sprintf(str,
            "HTTP/1.1 503 Service Unavailable\r\nContent-Type: "
            "text/html\r\nConnection: close\r\nDate: %s\r\n\r\n"
            "<html><head><title>503 Service Unavailable</title></head><body>"
            "<h1>503 Service Unavailable</h1><p>The server is temporarily "
            "unable to handle the request.</p></body></html>\r\n",
            currentTime);
    break;
  case 504:
    sprintf(str,
            "HTTP/1.1 504 Gateway Timeout\r\nContent-Type: "
            "text/html\r\nConnection: close\r\nDate: %s\r\n\r\n"
            "<html><head><title>504 Gateway Timeout</title></head><body>"
            "<h1>504 Gateway Timeout</h1><p>The server did not receive a "
            "timely response from the upstream server.</p></body></html>\r\n",
            currentTime);
    break;
  default:
    sprintf(str,
            "HTTP/1.1 500 Internal Server Error\r\nContent-Type: "
            "text/html\r\nConnection: close\r\nDate: %s\r\n\r\n"
            "<html><head><title>500 Internal Server Error</title></head><body>"
            "<h1>500 Internal Server Error</h1><p>An unexpected condition was "
            "encountered.</p></body></html>\r\n",
            currentTime);
    break;
  }
  send(socket, str, strlen(str), 0);
  return 0;
}

int connectRemoteServer(char *host_addr, int port_num) {
  int remoteSocket = socket(AF_INET, SOCK_STREAM, 0);

  if (remoteSocket < 0) {
    perror("Error in Creating Socket");
    return -1;
  }

  struct hostent *host = gethostbyname(host_addr);
  if (host == NULL) {
    fprintf(stderr, "No such host exists.\n");
    return -1;
  }

  struct sockaddr_in server_addr;
  bzero((char *)&server_addr, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port_num);

  bcopy((char *)host->h_addr, (char *)&server_addr.sin_addr.s_addr,
        host->h_length);

  // --- NEW LOGGING CODE ---
  struct in_addr **addr_list = (struct in_addr **)host->h_addr_list;
  if (addr_list[0] != NULL) {
    printf("Attempting to connect to IP: %s on port %d\n",
           inet_ntoa(*addr_list[0]), port_num);
  }
  // ------------------------

  if (connect(remoteSocket, (struct sockaddr *)&server_addr,
              (socklen_t)sizeof(server_addr)) < 0) {
    // perror prints our string, PLUS the exact system reason (e.g., "Connection
    // refused")
    perror("SYSTEM ERROR DURING CONNECT");
    return -1;
  }

  return remoteSocket;
}

int handle_request(int client_socketId, ParsedRequest *request, char *tempReq) {
  char *buf = (char *)malloc(sizeof(char) * MAX_BYTES);

  strcpy(buf, "GET ");
  strcat(buf, request->path);
  strcat(buf, " ");
  strcat(buf, request->version);
  strcat(buf, "\r\n");

  size_t len = strlen(buf);

  if (ParsedHeader_set(request, "Connection", "close") < 0) {
    printf("Set Header Key is not working\n");
  }

  if (ParsedHeader_get(request, "Host") == NULL) {
    if (ParsedHeader_set(request, "Host", request->host) < 0) {
      printf("Error setting Host header\n");
    }
  }

  if (ParsedRequest_unparse_headers(request, buf + len,
                                    (size_t)MAX_BYTES - len) < 0) {
    printf("Error unparsing header\n");
  }

  int server_port = 80;
  if (request->port != NULL) {
    server_port = atoi(request->port);
  }

  printf("Host = %s\n", request->host);
  printf("Port = %s\n", request->port ? request->port : "80");

  printf("handle_request: Connecting to host: %s, port: %d\n", request->host,
         server_port);
  int remoteSocketId = connectRemoteServer(request->host, server_port);
  if (remoteSocketId < 0) {
    free(buf);
    sendErrorMessage(client_socketId, 502);
    return -1;
  }
  printf("handle_request: Connected to remote server. Sending reconstructed "
         "request...\n");

  printf("Before send\n");
  int bytes_send = send(remoteSocketId, buf, strlen(buf), 0);
  printf("After send\n");
  printf("handle_request: Sent %d bytes to remote server. Waiting for "
         "response...\n",
         bytes_send);
  bzero(buf, MAX_BYTES);

  char *temp_buffer = (char *)malloc(sizeof(char) * MAX_BYTES);
  int temp_buffer_size = MAX_BYTES;
  int temp_buffer_index = 0;

  // BUG FIX #3: do the first recv before entering the loop,
  // so a 0 or error return is correctly skipped without entering.
  printf("Before recv\n");
  bytes_send = recv(remoteSocketId, buf, MAX_BYTES - 1, 0);
  printf("handle_request: Received %d bytes first chunk from remote server.\n",
         bytes_send);

  while (bytes_send > 0) {
    int bytes_written = send(client_socketId, buf, bytes_send, 0);
    printf("handle_request: Forwarded %d bytes to client.\n", bytes_written);

    // BUG FIX #4: check for recv error before copying / resizing
    if (bytes_written < 0) {
      perror("Error in sending to the client\n");
      break;
    }

    // Grow buffer if needed before copying
    if (temp_buffer_index + bytes_send >= temp_buffer_size) {
      temp_buffer_size += MAX_BYTES;
      temp_buffer = (char *)realloc(temp_buffer, temp_buffer_size);
    }

    for (int i = 0; i < bytes_send; i++) {
      temp_buffer[temp_buffer_index] = buf[i];
      temp_buffer_index++;
    }

    bzero(buf, MAX_BYTES);
    bytes_send = recv(remoteSocketId, buf, MAX_BYTES - 1, 0);
    printf("handle_request: Received next chunk of response (%d bytes)\n",
           bytes_send);
  }

  temp_buffer[temp_buffer_index] = '\0';
  free(buf);

  // BUG FIX #5: use temp_buffer_index (actual byte count) instead of strlen
  // so binary responses (images, etc.) are cached correctly
  add_cache_element(temp_buffer, temp_buffer_index, tempReq);
  free(temp_buffer);
  close(remoteSocketId);
  return 0;
}

int checkHTTPversion(char *msg) {
  int version = -1;
  if (strncmp(msg, "HTTP/1.1", 8) == 0) {
    version = 1;
  } else if (strncmp(msg, "HTTP/1.0", 8) == 0) {
    version = 1;
  } else {
    version = -1;
  }
  return version;
}

void *thread_fn(void *socketNew) {
  sem_wait(&semaphore);
  int p;
  sem_getvalue(&semaphore, &p);
  printf("semaphore value is : %d\n", p);
  int *t = (int *)socketNew;
  int socket = *t;
  int bytes_send_client, len;

  char *buffer = (char *)calloc(MAX_BYTES, sizeof(char));
  bzero(buffer, MAX_BYTES);
  bytes_send_client = recv(socket, buffer, MAX_BYTES, 0);

  while (bytes_send_client > 0) {
    len = strlen(buffer);
    if (strstr(buffer, "\r\n\r\n") == NULL) {
      bytes_send_client = recv(socket, buffer + len, MAX_BYTES - len, 0);
    } else {
      break;
    }
  }

  char *tempReq = (char *)malloc(strlen(buffer) + 1);
  strcpy(tempReq, buffer);

  struct cache_element *temp = find(tempReq);

  if (temp != NULL) {
    int size = temp->len;
    int pos = 0;
    char response[MAX_BYTES];

    // BUG FIX #6: fixed cache-hit sending loop so it sends exactly
    // 'size' bytes and doesn't overflow into garbage memory.
    while (pos < size) {
      int chunk = size - pos;
      if (chunk > MAX_BYTES)
        chunk = MAX_BYTES;
      bzero(response, MAX_BYTES);
      memcpy(response, temp->data + pos, chunk);
      send(socket, response, chunk, 0);
      pos += chunk;
    }
    printf("Data send to client successfully\n");
  } else if (bytes_send_client > 0) {
    len = strlen(buffer);
    ParsedRequest *request = ParsedRequest_create();

    if (ParsedRequest_parse(request, buffer, len) < 0) {
      printf("Error parsing request\n");
    } else {
      bzero(buffer, MAX_BYTES);
      if (!strcmp(request->method, "GET")) {
        if (request->host && request->path &&
            checkHTTPversion(request->version) == 1) {
          bytes_send_client = handle_request(socket, request, tempReq);
          if (bytes_send_client == -1) {
            sendErrorMessage(socket, 500);
          }
        } else {
          sendErrorMessage(socket, 400);
        }
      } else {
        printf("This code only handles GET method\n");
        sendErrorMessage(socket, 405); // now correctly sends 405
      }
    }
    ParsedRequest_destroy(request);
  } else if (bytes_send_client == 0) {
    printf("Client Disconnected\n");
  }

  shutdown(socket, SHUT_RDWR);
  close(socket);
  free(buffer);
  sem_post(&semaphore);
  sem_getvalue(&semaphore, &p);
  printf("Semaphore value is : %d\n", p);
  free(tempReq);
  return NULL;
}

int main(int argc, char *argv[]) {
  setvbuf(stdout, NULL, _IONBF, 0);
  setvbuf(stderr, NULL, _IONBF, 0);

  int client_socketId;
  int client_len;

  struct sockaddr_in server_addr, client_addr;

  sem_init(&semaphore, 0, MAX_CLIENTS);
  pthread_mutex_init(&lock, NULL);

  if (argc == 2) {
    port_number = atoi(argv[1]);
  } else {
    printf("Too few arguments\n");
    exit(1);
  }

  printf("Starting proxy server on port %d\n", port_number);

  proxy_socketId = socket(AF_INET, SOCK_STREAM, 0);

  if (proxy_socketId < 0) {
    printf("Error creating socket\n");
    exit(1);
  }

  int reuse = 1;
  if (setsockopt(proxy_socketId, SOL_SOCKET, SO_REUSEADDR, (const char *)&reuse,
                 sizeof(reuse)) < 0) {
    printf("Error setting socket options\n");
    exit(1);
  }

  bzero((char *)&server_addr, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port_number);
  server_addr.sin_addr.s_addr = INADDR_ANY;

  if (bind(proxy_socketId, (struct sockaddr *)&server_addr,
           sizeof(server_addr)) < 0) {
    printf("Error binding socket\n");
    exit(1);
  }

  printf("Binding on port %d.\n", port_number);

  int listen_status = listen(proxy_socketId, MAX_CLIENTS);
  if (listen_status < 0) {
    printf("Error listening on socket\n");
    exit(1);
  }

  // BUG FIX #8: use i modulo MAX_CLIENTS to prevent buffer overflow
  // after more than MAX_CLIENTS total connections.
  int i = 0;
  int Connected_socketId[MAX_CLIENTS];

  while (1) {
    bzero((char *)&client_addr, sizeof(client_addr));

    client_len = sizeof(client_addr);
    client_socketId = accept(proxy_socketId, (struct sockaddr *)&client_addr,
                             (socklen_t *)&client_len);

    if (client_socketId < 0) {
      printf("Error accepting client connection\n");
      continue;
    } else {
      Connected_socketId[i % MAX_CLIENTS] = client_socketId;
    }

    struct sockaddr_in *client_ptr = (struct sockaddr_in *)&client_addr;
    struct in_addr ip_addr = client_ptr->sin_addr;
    char str[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &ip_addr, str, INET_ADDRSTRLEN);
    printf("Client is connected with port number %d and IP address is %s\n",
           ntohs(client_ptr->sin_port), str);

    pthread_create(&tid[i % MAX_CLIENTS], NULL, thread_fn,
                   (void *)&Connected_socketId[i % MAX_CLIENTS]);
    i++;
  }

  close(proxy_socketId);
  return 0;
}