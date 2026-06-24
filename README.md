# 🚀 Multithreaded Proxy Server with LRU Cache

A high-performance **multithreaded HTTP proxy server** built in **C** that intercepts client requests, forwards them to remote web servers, and caches responses using a **thread-safe Least Recently Used (LRU) cache**.

The proxy reduces network latency and bandwidth usage by serving frequently requested resources directly from memory while efficiently handling multiple client connections concurrently.

---

## ✨ Features

### 🔹 Concurrent Request Handling

* Uses **POSIX Threads (pthreads)** to process multiple client requests simultaneously.
* Each incoming connection is handled by a dedicated worker thread.

### 🔹 Thread-Safe LRU Cache

* Custom **LRU Cache** implemented using a doubly linked list.
* Enforces cache size limits and automatic eviction of least recently used entries.
* Protected using **Mutex Locks (`pthread_mutex_t`)** to prevent race conditions during concurrent access.

### 🔹 Connection Management

* Uses **Semaphores (`sem_t`)** to limit the number of active client connections.
* Prevents excessive resource consumption and improves server stability.

### 🔹 Raw Socket Programming

* Built directly on top of Linux socket APIs:

  * `sys/socket.h`
  * `netinet/in.h`
  * `netdb.h`
* Handles TCP communication, host resolution, and packet forwarding.

### 🔹 Custom HTTP Request Parser

* Supports HTTP/1.0 and HTTP/1.1 GET requests.
* Parses and reconstructs HTTP headers.
* Modifies connection headers to ensure proper proxy behavior.

---

## 🧠 System Architecture

```text
          ┌─────────────┐
          │   Client    │
          └──────┬──────┘
                 │
                 ▼
        ┌─────────────────┐
        │  Proxy Server   │
        └──────┬──────────┘
               │
      ┌────────┴────────┐
      │                 │
      ▼                 ▼
 Cache Hit         Cache Miss
      │                 │
      ▼                 ▼
Return Data      Remote Server
From Cache       Connection
      │                 │
      └────────┬────────┘
               ▼
         Store in Cache
               │
               ▼
            Client
```

---

## ⚙️ Workflow

### 1. Listen

The main thread binds to a specified port and listens for incoming TCP connections.

### 2. Accept & Delegate

When a client connects:

* Semaphore availability is checked.
* A worker thread is created to process the request.

### 3. Parse & Cache Lookup

The worker thread:

* Parses the incoming HTTP request.
* Checks whether the requested URL already exists in the cache.

#### Cache Hit

* Response is served directly from memory.
* LRU metadata is updated.

#### Cache Miss

* Hostname is resolved using `gethostbyname()`.
* TCP connection is established with the remote server.
* Request is forwarded upstream.

### 4. Fetch & Store

* Response is streamed back to the client.
* Data is simultaneously stored in the LRU cache for future requests.

---

## 🛠️ Tech Stack

| Category         | Technology               |
| ---------------- | ------------------------ |
| Language         | C                        |
| Concurrency      | POSIX Threads (pthreads) |
| Synchronization  | Mutexes, Semaphores      |
| Networking       | POSIX Sockets, TCP/IP    |
| Operating System | Linux (Ubuntu/Debian)    |

---

## 🚀 Installation

### Prerequisites

```bash
sudo apt update
sudo apt install build-essential
```

### Clone Repository

```bash
git clone https://github.com/harshroyy/Multithreaded-Proxy-Server.git

cd Multithreaded-Proxy-Server
```

### Build

```bash
make clean
make all
```

### Run

```bash
./proxy 8080
```

Expected Output:

```text
Starting proxy server on port 8080
Binding on port 8080
Proxy server is running...
```

---

## 🧪 Testing

Open a second terminal and send requests through the proxy.

### First Request (Cache Miss)

```bash
curl -x http://127.0.0.1:8080 http://neverssl.com/
```

Server behavior:

```text
Request received
Cache Miss
Connecting to remote server...
Fetching data...
Caching response...
```

### Second Request (Cache Hit)

```bash
curl -x http://127.0.0.1:8080 http://neverssl.com/
```

Server behavior:

```text
Request received
Data retrieved from cache
```

---

## 📊 Key Concepts Demonstrated

* Multithreaded Server Design
* Thread Synchronization
* LRU Cache Implementation
* Socket Programming
* HTTP Protocol Handling
* Producer–Consumer Style Concurrency
* Network Traffic Optimization

---

## 📈 Future Improvements

### HTTPS Support

Implement the `CONNECT` method to support encrypted TLS traffic.

### Multiprocessing

Combine process-based and thread-based concurrency to leverage multiple CPU cores.

### Extended HTTP Support

Add support for:

* POST
* PUT
* DELETE
* Persistent Connections

### Advanced Caching

* Cache expiration policies
* Conditional requests
* Disk-based caching

---

## 👨‍💻 Author

**Harsh Raj**

Built as a systems programming project to explore:

* Computer Networks
* Operating Systems
* Multithreading
* Cache Management
* Low-Level Networking
