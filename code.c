#define _POSIX_C_SOURCE 199309L
#define _GNU_SOURCE
#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <sched.h> 
#include <unistd.h>

// Holds an IP address and port number for one network endpoint
typedef struct ADDR {
    char *ip;
    int port;
} addr;


// Arguments passed to each master worker thread.
// Each thread gets its own slice of the matrix to send to one slave.
// TO ADD: COMP ARGS
typedef struct ARGS {
    int rows;       
    int start_row;   
    int n;           
    addr *slave;     
    float **X;      
    int q;
    float weight_sum;
    float *w;
    float *p;               
    pthread_mutex_t *mutex;
} args;




// ------------------------------------------------------------------
// Helper Functions -------------------------------------------------
// ------------------------------------------------------------------

// recv_all -> Receive exactly size bytes into buffer.
// Blocks until all bytes arrive or an error occurs.
void recv_all(int sock, void *buffer, size_t size) {
    size_t total = 0;
    while (total < size) {
        /* recv() returns number of bytes read, 0 on close, -1 on error */
        int r = recv(sock, (char*)buffer + total, size - total, 0);
        if (r <= 0) {
            perror("recv_all failed");
            exit(1);
        }
        total += r;
    }
}


// send_all —> Send exactly size bytes from buffer.
// Blocks until all bytes are sent or an error occurs.
void send_all(int sock, void *buffer, size_t size) {
    size_t total = 0;
    while (total < size) {
        int s = send(sock, (char*)buffer + total, size - total, 0);
        if (s <= 0) {
            perror("send_all failed");
            exit(1);
        }
        total += s;
    }
}


// MSE Computation Function
// Computes for the MSE
// mxn Square Matrix X; vector w; result vector p; int q, int m, int n 
void *mse_wma(void *arg){
    // Initialize variables
    args *data = (args*)arg;
    float** X = data->X;
    float* w = data->w; 
    float* p = data->p;
    float weight_sum = data->weight_sum;
    int q = data->q;
    int m = data->m; 
    int start = data->start_row;
    int end = data->end_row;
    pthread_mutex_t *mutex = data->mutex;

    // j - column, i - row
    if (end <= q) return NULL; // protection against division by zero
   
    // O(n^3)
    for(int j = 0; j < m; j++){
        // ----------------------------------
        float local_sum = 0; // the local sum

        // max(start, q)
        int index_start = (start > q) ? start : q;

        if (index_start >= end) continue;

        // O(n^2)
        for(int i = index_start; i < end; i++){
            float weighted = 0; // weighted sum
            // Computes for the WMA
            // Initialized WMA for i = q
            // O(q) -> O(n)
            for (int k = 0; k < q; k++){
                weighted  += (w[k] * X[i- q + k][j]);
            }   

            // Compute for the mse
            float wma_result = weighted/weight_sum; 
            float sum_difference = X[i][j] - wma_result; 
            local_sum += sum_difference * sum_difference;

        }

        // Update p[j] with lock protection
        pthread_mutex_lock(mutex);
        p[j] += (sqrt(local_sum)/(m - index_start));
        pthread_mutex_unlock(mutex);
    }

    return NULL;
}



// MASTER WORKER THREAD
// Where each thread is responsible for sending one submatrix to one slave and 
// thread opens its own dedicated TCP connection to a single slave.
void *worker(void *arg) {
    args *a = (args *)arg;

    // Create a TCP socket
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("Socket creation failed");
        pthread_exit(NULL);
    }

    // Configure the slave's address
    struct sockaddr_in slave_addr = {0};
    slave_addr.sin_family = AF_INET;   // IPv4 
    slave_addr.sin_port   = htons(a->slave->port);  // host-to-network byte order

    // Convert dotted-decimal string
    if (inet_pton(AF_INET, a->slave->ip, &slave_addr.sin_addr) <= 0) {
        perror("Invalid IP address");
        close(sock);
        pthread_exit(NULL);
    }

    // Connect to the slave 
    if (connect(sock, (struct sockaddr*)&slave_addr, sizeof(slave_addr)) < 0) {
        perror("Connection to slave failed");
        close(sock);
        pthread_exit(NULL);
    }

    printf("[Thread] Connected to slave (%s:%d)\n", a->slave->ip, a->slave->port);

    // Send metadata (rows and columns count)
    // htonl() converts 32-bit int from host byte order to network byte order
    int net_rows = htonl(a->rows);
    send_all(sock, &net_rows, sizeof(int));

    int net_n = htonl(a->n);
    send_all(sock, &net_n, sizeof(int));

    // Send the submatrix row by row
    // We send rows [start_row ... start_row + rows - 1] of the full matrix X.
    // Each row is an array of n floats = n * sizeof(float) bytes.
    // Floats are sent as raw bytes
    for (int r = 0; r < a->rows; r++) {
        send_all(sock, a->X[a->start_row + r], sizeof(float) * a->n);
    }

    int net_n = htonl(a->n);
    send_all(sock, &net_n, sizeof(int));
    
    // Send q
    int net_q = htonl(a->q);
    send_all(sock, &net_q, sizeof(int));
    // Send weight_sum
    send_all(sock, &a->weight_sum, sizeof(float));
    // Send the w array
    send_all(sock, a->w, sizeof(float) * a->q);

    printf("[Thread] Submatrix rows %d-%d sent to slave (%s:%d)\n",
           a->start_row, a->start_row + a->rows - 1,
           a->slave->ip, a->slave->port);

    // Receive ACK from slave 
    char ack[4] = {0};
    recv_all(sock, ack, 3);
    ack[3] = '\0';

    if (strncmp(ack, "ack", 3) == 0) {
        printf("[Thread] ACK received from slave (%s:%d)\n",
               a->slave->ip, a->slave->port);
    } else {
        fprintf(stderr, "[Thread] WARNING: Invalid ACK from slave (%s:%d): '%s'\n",
                a->slave->ip, a->slave->port, ack);
    }

    close(sock);
    pthread_exit(NULL);
}

// MASTER FUNCTION
// Spawns t threads (one per slave) to distribute submatrices in parallel.
// Records elapsed time from first thread launch to last thread completion.
void master(int n, int t, addr **slave_addresses, float **X, int q, 
    float weight_sum, float *w, float *p, pthread_mutex_t *mutex) {
    struct timespec start_time, end_time;

    pthread_t threads[t];
    args thread_args[t];

    // Divide n rows among t slaves.
    // If n is not divisible by t, distribute the remainder one extra row
    // at a time to the first remainder slaves.
    int rows_per_thread = n / t;
    int remainder       = n % t;
    int current_row     = 0;

    // FOR WITH CORE AFFINITY
    // Get the total number of processors
    int total_cores = sysconf(_SC_NPROCESSORS_ONLN); 
    int usable_cores = (total_cores > 1) ? total_cores - 1 : 1; 
    // time_before: record just before launching all threads 
    clock_gettime(CLOCK_MONOTONIC, &start_time);

    // Launch one thread per slave 
    for (int i = 0; i < t; i++) {
        int rows = rows_per_thread + (i < remainder ? 1 : 0);

        thread_args[i].rows         = rows;
        thread_args[i].start_row    = current_row;
        thread_args[i].n            = n;
        thread_args[i].slave        = slave_addresses[i];
        thread_args[i].X            = X;
        thread_args[i].q            = q;
        thread_args[i].weight_sum   = weight_sum;
        thread_args[i].w            = w;
        thread_args[i].p            = p;
        thread_args[i].mutex        = mutex;

        // FOR WITH CORE AFFINITY
        // Initialized attribute variable for core
        pthread_attr_t attr;
        pthread_attr_init(&attr);

        // Assign to cores
        int core = i % usable_cores;
        cpu_set_t cpuset; 
        CPU_ZERO(&cpuset); 
        CPU_SET(core, &cpuset); 

        // Set the attribute in binding this specific thread to the defined CPU set
        int a = pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
        if (a != 0) {
            fprintf(stderr, "Error in setting the affinity: %d\n", a);
        }

        // Create and initialize the thread with the worker function and arguments
        if (pthread_create(&threads[i], &attr, worker, &thread_args[i]) != 0) {
            perror("pthread_create failed");
            exit(1);
        }
        
        // Cleanup attribute variable after successful thread creation
        pthread_attr_destroy(&attr);

        current_row += rows;
    }

    // Wait for all threads to finish like when all ACKs received
    for (int i = 0; i < t; i++) {
        pthread_join(threads[i], NULL);
    }

    // time_after: all ACKs received 
    clock_gettime(CLOCK_MONOTONIC, &end_time);

    // Compute elapsed time in seconds with nanosecond precision
    double elapsed =
        (end_time.tv_sec  - start_time.tv_sec) +
        (end_time.tv_nsec - start_time.tv_nsec) / 1e9;

    printf("\n[Master] All slaves have acknowledged.\n");
    printf("[Master] time_elapsed = %.9f seconds\n", elapsed);
}

// SLAVE FUNCTION
// Listens on port p for the master to connect.
// Receives a submatrix, acknowledges, then prints it for verification.
// Records and prints its own elapsed time which is time to receive + send ACK.
void slave(int n, int p) {
    // Create a TCP server socket.
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("Server socket creation failed");
        exit(1);
    }

    // SO_REUSEADDR: Allows reusing the port immediately after program exit.
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // Bind socket to all interfaces on port p 
    struct sockaddr_in server_addr = {0};
    server_addr.sin_family      = AF_INET;
    server_addr.sin_port        = htons(p);
    server_addr.sin_addr.s_addr = INADDR_ANY;  // Accept connections on any local IP

    if (bind(server_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("Bind failed");
        close(server_fd);
        exit(1);
    }

    // listen() marks the socket as passive, ready to accept connections
    if (listen(server_fd, 5) < 0) {
        perror("Listen failed");
        close(server_fd);
        exit(1);
    }

    printf("[Slave] Listening on port %d...\n", p);

    while (1) {
        // accept() blocks until master connects; returns a new connected socket 
        int sock = accept(server_fd, NULL, NULL);
        if (sock < 0) {
            perror("Accept failed");
            continue;
        }

        printf("[Slave] Master connected. Receiving data...\n");

        // Receive metadata
        int net_rows, net_cols;
        recv_all(sock, &net_rows, sizeof(int));
        int rows = ntohl(net_rows);   // network-to-host byte order

        recv_all(sock, &net_cols, sizeof(int));
        int n_cols = ntohl(net_cols);

        // Receive q
        int net_q;
        recv_all(sock, &net_q, sizeof(int));
        int q = ntohl(net_q);
        
        // Receive weight_sum
        float weight_sum;
        recv_all(sock, &weight_sum, sizeof(float));

        // Allocate and receive the w vector
        float *w = malloc(q * sizeof(float));
        if (!w) { perror("malloc w"); exit(1); }
        recv_all(sock, w, sizeof(float) * q);

        // Dynamically allocate the submatrix.
        // rows x n_cols floats.
        float **submatrix = malloc(rows * sizeof(float*));
        if (!submatrix) { perror("malloc"); exit(1); }

        for (int i = 0; i < rows; i++) {
            submatrix[i] = malloc(n_cols * sizeof(float));
            if (!submatrix[i]) { perror("malloc row"); exit(1); }
            recv_all(sock, submatrix[i], sizeof(float) * n_cols);
        }

        printf("[Slave] Received submatrix: %d rows x %d cols\n", rows, n_cols);

        struct timespec time_before, time_after;

        // time_before: the moment the slave received the rows and before computation
        clock_gettime(CLOCK_MONOTONIC, &time_before);

        // Send ACK to master
        send_all(sock, "ack", 3);

        // time_after: immediately after sending ACK.
        clock_gettime(CLOCK_MONOTONIC, &time_after);

        double elapsed =
            (time_after.tv_sec  - time_before.tv_sec) +
            (time_after.tv_nsec - time_before.tv_nsec) / 1e9;

        printf("[Slave] time_elapsed = %.9f seconds\n", elapsed);


        // Prints the received submatrix for verification
        printf("\n[Slave] === Submatrix Verification ===\n");
        if (rows <= 10 && n_cols <= 10) {
            // Print full matrix for small sizes 
            for (int i = 0; i < rows; i++) {
                printf("  Row %d: ", i);
                for (int j = 0; j < n_cols; j++) {
                    printf("%6.1f ", submatrix[i][j]);
                }
                printf("\n");
            }
        } else {
            printf("  First element [0][0]     = %.1f\n", submatrix[0][0]);
            printf("  Last  element [%d][%d] = %.1f\n",
                   rows - 1, n_cols - 1, submatrix[rows-1][n_cols-1]);
        }
        printf("[Slave] === End Verification ===\n\n");

        // Free the allocated submatrix 
        for (int i = 0; i < rows; i++) free(submatrix[i]);
        free(submatrix);

        close(sock);

        // Break after one connection
        break;
    }

    close(server_fd);
}

// MAIN FUNCTION
// Reads n, p, s from the user and dispatches to master or slave mode.
int main() {
    int n, p, s, q;

    // Read user inputs
    printf("Enter size of square matrix n: ");
    scanf("%d", &n);
    printf("Enter port number p: ");
    scanf("%d", &p);
    printf("Enter instance status s [0 = master | 1 = slave]: ");
    scanf("%d", &s);

    srand((unsigned int)time(NULL));

    // MASTER BRANCH (s == 0)
    if (s == 0) {

        // Allocate and populate nxn matrix with random positive ints 
        float **X = (float**)malloc(sizeof(float*) * n);
        if (!X) { fprintf(stderr, "Memory allocation failed\n"); return 1; }

        for (int i = 0; i < n; i++) {
            X[i] = (float*)malloc(sizeof(float) * n);
            if (!X[i]) { fprintf(stderr, "Memory allocation failed\n"); return 1; }
            for (int j = 0; j < n; j++) {
                X[i][j] = (float)((rand() % 100) + 1);
            }
        }

        float a, b, b1, b2;
        // q = max(s1*10 + s2, s1*s2)
        // S_1 = 9, S_2 = 6
        a = 9 * 10 + 6;
        b = 9 * 6;
        q = a > b? a : b; // max(a, b)

        // Creating the vector w
        float *w = (float*)malloc(sizeof(float)*q);
        if (w == NULL) {
            printf("Memory allocation failed");
            return 1;
        }

        // Populate the q * 1 vector using random numbers
        for(int i = 0; i < q; i++){
            w[i] = (rand() % (100 - 1 + 1)) + 1;
        } 

        // Allocate memory for n vector p
        float *p = (float*)malloc(sizeof(float) * n);
        if (!p) { fprintf(stderr, "Memory allocation failed\n"); return 1; }

        // Initialize vector p to zero
        for (int i = 0; i < n; i++) {
            p[i] = 0;           
        }
    

        printf("[Master] Generated %dx%d matrix.\n", n, n);

        // Read configuration file
        FILE *file = fopen("config.txt", "r");
        if (!file) {
            fprintf(stderr, "[Master] Error: Cannot open config.txt\n");
            return 1;
        }

        /*
         * config.txt format:
         *   MASTER <ip> <port>
         *   <num_slaves>
         *   SLAVE <ip1> <port1>
         *   SLAVE <ip2> <port2>
         *   ...
         */

        // Read master's own IP and port for storage
        addr *master_address = malloc(sizeof(addr));
        master_address->ip = malloc(16);  // Max IPv4 string: "255.255.255.255\0" = 16 chars
        if (fscanf(file, " MASTER %15s %d", master_address->ip, &master_address->port) != 2) {
            fprintf(stderr, "[Master] Failed to read MASTER config\n");
            return 1;
        }
        printf("[Master] Self — %s:%d\n", master_address->ip, master_address->port);

        // Read how many slaves exist 
        int num_slaves = 0;
        if (fscanf(file, " %d", &num_slaves) != 1 || num_slaves <= 0) {
            fprintf(stderr, "[Master] Failed to read slave count\n");
            return 1;
        }
        printf("[Master] Number of slaves: %d\n", num_slaves);

        // Read each slave's IP and port
        addr **slave_addresses = malloc(sizeof(addr*) * num_slaves);
        for (int i = 0; i < num_slaves; i++) {
            slave_addresses[i] = malloc(sizeof(addr));
            slave_addresses[i]->ip = malloc(16);
            if (fscanf(file, " SLAVE %15s %d",
                       slave_addresses[i]->ip,
                       &slave_addresses[i]->port) != 2) {
                fprintf(stderr, "[Master] Failed to read SLAVE %d config\n", i);
                return 1;
            }
            printf("[Master] Slave %d — %s:%d\n",
                   i, slave_addresses[i]->ip, slave_addresses[i]->port);
        }

        fclose(file);

        // Distribute submatrices to slaves and collect ACKs
        // the master function handles spawning threads, timing, and joining.
        master(n, num_slaves, slave_addresses, X, w, q, weight_sum, p);

        // Cleanup
        for (int i = 0; i < num_slaves; i++) {
            free(slave_addresses[i]->ip);
            free(slave_addresses[i]);
        }
        free(slave_addresses);
        free(master_address->ip);
        free(master_address);
        for (int i = 0; i < n; i++) free(X[i]);
        free(X);

    }

    // SLAVE RBANCH
    else if (s == 1) {

        // Read master IP from config file
        FILE *file = fopen("config.txt", "r");
        if (!file) {
            fprintf(stderr, "[Slave] Error: Cannot open config.txt\n");
            return 1;
        }

        char master_ip[20];
        int master_port;
        if (fscanf(file, " MASTER %19s %d", master_ip, &master_port) != 2) {
            fprintf(stderr, "[Slave] Failed to read MASTER config\n");
            fclose(file);
            return 1;
        }
        printf("[Slave] Master is at %s:%d\n", master_ip, master_port);

        // slave list is not needed, close the file
        fclose(file);

        
        //Wait for master, receive submatrix, send ACK, time it
        slave(n, p);

    } else {
        fprintf(stderr, "Invalid status s=%d. Use 0 (master) or 1 (slave).\n", s);
        return 1;
    }

    return 0;
}