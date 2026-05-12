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


// Holds an IP address and port number for one network endpoint
typedef struct ADDR {
    char *ip;
    int port;
} addr;


// Arguments passed to each master worker thread.
// Each thread gets its own slice of the matrix to send to one slave
// and receives the corresponding partial p vector back.
typedef struct ARGS {
    int rows;              
    int start_row;          
    int n;                  
    addr *slave; // Slave's IP and port
    float **X;              
    int q;                 
    float weight_sum;       
    float *w;               
    float *p;               
    pthread_mutex_t *mutex; 
} args;



// Helper Functions 

// recv_all: Receive exactly size bytes into buffer.
// Blocks until all bytes arrive or an error occurs.
void recv_all(int sock, void *buffer, size_t size) {
    size_t total = 0;
    while (total < size) {
        int r = recv(sock, (char*)buffer + total, size - total, 0);
        if (r <= 0) {
            perror("recv_all failed");
            exit(1);
        }
        total += r;
    }
}


// send_all: Send exactly size bytes from buffer.
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



// WMA + MSE Computation 
void mse_wma(float **X, int m, int n, int q, float weight_sum,
             float *w, float *p_out) {

    // Safety guard because we need at least q+1 rows to compute one WMA value
    if (m <= q) {
        for (int j = 0; j < n; j++) {
            p_out[j] = 0.0f;
        }
        return;
    }

    // Process each column independently
    for (int j = 0; j < n; j++) {
        float local_sum = 0.0f; // the local sum

        // Compute WMA for rows i from q to m-1
        for (int i = q; i < m; i++) {
            float weighted = 0.0f; // weighted sum
            
            // Computes for the WMA
            for (int k = 0; k < q; k++) {
                weighted += (w[k] * X[i - q + k][j]);
            }   

            // Compute for the mse
            float wma_result = weighted / weight_sum; 
            float sum_difference = X[i][j] - wma_result; 
            local_sum += sum_difference * sum_difference;
        }

        // p_out[j] stores the MSE for column j
        p_out[j] = sqrtf(local_sum) / (m - q);
    }
}



// Master Worker Thread for the master
//  Each thread: Connects to its assigned slave.
//  Sends: metadata (rows, n), submatrix rows, then WMA params (q, weight_sum, w).
//  Receives "ack" from the slave.
//  Receives the slave's computed partial p vector.
//  Merges the received p into the shared p using mutex-protected accumulation.
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
    slave_addr.sin_family = AF_INET;
    slave_addr.sin_port   = htons(a->slave->port);

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

    // Send metadata: number of rows and number of columns
    int net_rows = htonl(a->rows);
    send_all(sock, &net_rows, sizeof(int));

    int net_n = htonl(a->n);
    send_all(sock, &net_n, sizeof(int));

    // Send the submatrix row by row
    // Sends rows of the full matrix X
    for (int r = 0; r < a->rows; r++) {
        send_all(sock, a->X[a->start_row + r], sizeof(float) * a->n);
    }

    // Send WMA parameters: q, weight_sum, w
    // Sent after the submatrix to match the slave's receive order
    int net_q = htonl(a->q);
    send_all(sock, &net_q, sizeof(int));

    send_all(sock, &a->weight_sum, sizeof(float));

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

    // Receive the slave's partial p vector (n floats, one per column)
    // The slave computes MSE for all n columns across its row slice.
    // We accumulate into the shared p vector under mutex protection
    // because multiple threads write to different non-overlapping indices
    float *p_partial = malloc(sizeof(float) * a->n);
    if (!p_partial) { 
        perror("malloc p_partial"); 
        pthread_exit(NULL); 
    }
    recv_all(sock, p_partial, sizeof(float) * a->n);

    // Write received partial p into shared p vector with mutex protection
    pthread_mutex_lock(a->mutex);
    for (int j = 0; j < a->n; j++) {
        a->p[j] += p_partial[j];
    }
    pthread_mutex_unlock(a->mutex);

    free(p_partial);
    close(sock);
    pthread_exit(NULL);
}



// Master Function 

// master: Distributes submatrices, collects partial p, reports time
// Spawns t threads (one per slave) to send submatrices in parallel and
// receive back the partial MSE vectors and records the total time_elapsed from
// first thread launch to last join (full communication + computation)
void master(int n, int t, addr **slave_addresses, float **X,
            int q, float weight_sum, float *w, float *p) {

    struct timespec start_time, end_time;

    pthread_t threads[t];
    args thread_args[t];
    pthread_mutex_t mutex;
    pthread_mutex_init(&mutex, NULL);

    // Divide n rows among t slaves
    // Remainder rows are distributed one extra to the first remainder slaves
    int rows_per_thread = n / t;
    int remainder = n % t;
    int current_row = 0;

    // Core affinity setup
    // Reserve core 0 for the main/master thread; assign worker threads to cores 1+
    int total_cores = sysconf(_SC_NPROCESSORS_ONLN);
    int usable_cores = (total_cores > 1) ? total_cores - 1 : 1;

    // time_before: recorded just before launching all threads 
    clock_gettime(CLOCK_MONOTONIC, &start_time);

    // Launch one thread per slave
    for (int i = 0; i < t; i++) {
        int rows = rows_per_thread + (i < remainder ? 1 : 0);

        thread_args[i].rows = rows;
        thread_args[i].start_row = current_row;
        thread_args[i].n = n;
        thread_args[i].slave = slave_addresses[i];
        thread_args[i].X = X;
        thread_args[i].q = q;
        thread_args[i].weight_sum = weight_sum;
        thread_args[i].w = w;
        thread_args[i].p = p;
        thread_args[i].mutex = &mutex;

        // Core affinity: pin thread i to core (i % usable_cores) + 1
        pthread_attr_t attr;
        pthread_attr_init(&attr);

        int core = (i % usable_cores) + 1;
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(core, &cpuset);

        int rc = pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            fprintf(stderr, "[Master] Warning: affinity set failed for thread %d: %d\n", i, rc);
        }

        if (pthread_create(&threads[i], &attr, worker, &thread_args[i]) != 0) {
            perror("pthread_create failed");
            exit(1);
        }

        pthread_attr_destroy(&attr);
        current_row += rows;
    }

    // Wait for ALL threads to complete (all ACKs received and p vector assembled)
    for (int i = 0; i < t; i++) {
        pthread_join(threads[i], NULL);
    }

    // time_after 
    clock_gettime(CLOCK_MONOTONIC, &end_time);

    double elapsed =
        (end_time.tv_sec  - start_time.tv_sec) +
        (end_time.tv_nsec - start_time.tv_nsec) / 1e9;

    printf("\n[Master] All slaves have completed.\n");
    printf("[Master] time_elapsed = %.9f seconds\n", elapsed);

    pthread_mutex_destroy(&mutex);
}


// Slave Function
// slave receives submatrix + WMA params, computes MSE, returns p
void slave(int n, int p) {
    // Create a TCP server socket
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("Server socket creation failed");
        exit(1);
    }

    // SO_REUSEADDR: avoids Address already in use on quick restarts
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // Bind to all interfaces on port p
    struct sockaddr_in server_addr = {0};
    server_addr.sin_family      = AF_INET;
    server_addr.sin_port        = htons(p);
    server_addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("Bind failed");
        close(server_fd);
        exit(1);
    }

    if (listen(server_fd, 5) < 0) {
        perror("Listen failed");
        close(server_fd);
        exit(1);
    }

    printf("[Slave] Listening on port %d...\n", p);

    while (1) {
        // Block until master connects
        int sock = accept(server_fd, NULL, NULL);
        if (sock < 0) {
            perror("Accept failed");
            continue;
        }

        printf("[Slave] Master connected. Receiving data...\n");

        // Receive metadata 
        int net_rows, net_cols;
        recv_all(sock, &net_rows, sizeof(int));
        int rows = ntohl(net_rows);

        recv_all(sock, &net_cols, sizeof(int));
        int n_cols = ntohl(net_cols);

        // Receive submatrix (rows x n_cols floats)
        float **submatrix = malloc(rows * sizeof(float*));
        if (!submatrix) { perror("malloc submatrix"); exit(1); }

        for (int i = 0; i < rows; i++) {
            submatrix[i] = malloc(n_cols * sizeof(float));
            if (!submatrix[i]) { perror("malloc submatrix row"); exit(1); }
            recv_all(sock, submatrix[i], sizeof(float) * n_cols);
        }

        // Receive WMA parameters
        int net_q;
        recv_all(sock, &net_q, sizeof(int));
        int q = ntohl(net_q);

        float weight_sum;
        recv_all(sock, &weight_sum, sizeof(float));

        float *w = malloc(q * sizeof(float));
        if (!w) { perror("malloc w"); exit(1); }
        recv_all(sock, w, sizeof(float) * q);

        printf("[Slave] Received submatrix: %d rows x %d cols, q=%d\n", rows, n_cols, q);

        // Submatrix verification 
        printf("\n[Slave] === Submatrix Verification ===\n");
        if (rows <= 10 && n_cols <= 10) {
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

        // Send ACK to master 
        // ACK is sent BEFORE computation so the master's thread can proceed
        // to the recv phase and wait - the master and slave then work in lockstep
        send_all(sock, "ack", 3);

        // Allocate result vector p_out (one value per column)
        float *p_out = calloc(n_cols, sizeof(float));
        if (!p_out) { perror("calloc p_out"); exit(1); }

        struct timespec time_before, time_after;

        // time_before: AFTER receiving data, BEFORE computation 
        clock_gettime(CLOCK_MONOTONIC, &time_before);

        // Compute WMA-based RMSE for each column (row-wise sliding window)
        mse_wma(submatrix, rows, n_cols, q, weight_sum, w, p_out);

        // time_after: AFTER computation, BEFORE sending
        clock_gettime(CLOCK_MONOTONIC, &time_after);

        double elapsed =
            (time_after.tv_sec  - time_before.tv_sec) +
            (time_after.tv_nsec - time_before.tv_nsec) / 1e9;

        printf("[Slave] time_elapsed (computation only) = %.9f seconds\n", elapsed);

        // Send p_out back to master (M1PR: slave → master) 
        send_all(sock, p_out, sizeof(float) * n_cols);

        // Cleanup
        for (int i = 0; i < rows; i++) free(submatrix[i]);
        free(submatrix);
        free(w);
        free(p_out);

        close(sock);
        break;  // One connection per slave per run
    }

    close(server_fd);
}



// Main Function
int main() {
    int n, p, s;

    // Read user inputs:
    printf("Enter size of square matrix n: ");
    scanf("%d", &n);
    printf("Enter port number p: ");
    scanf("%d", &p);
    printf("Enter instance status s [0 = master | 1 = slave]: ");
    scanf("%d", &s);

    srand((unsigned int)time(NULL));

    // MASTER BRANCH
    if (s == 0) {

        // Allocate and populate n x n matrix with random positive ints [1, 100]
        float **X = (float**)malloc(sizeof(float*) * n);
        if (!X) { fprintf(stderr, "Memory allocation failed\n"); return 1; }

        for (int i = 0; i < n; i++) {
            X[i] = (float*)malloc(sizeof(float) * n);
            if (!X[i]) { fprintf(stderr, "Memory allocation failed\n"); return 1; }
            for (int j = 0; j < n; j++) {
                X[i][j] = (float)((rand() % 100) + 1);
            }
        }

        // Compute q = max(S1*10 + S2, S1*S2) where S1=9, S2=6
        float a_val = 9 * 10 + 6;   // 96
        float b_val = 9 * 6;         // 54
        int q = (int)((a_val > b_val) ? a_val : b_val);  // 96

        // Create weight vector w of length q with random positive values
        float *w = (float*)malloc(sizeof(float) * q);
        if (!w) { fprintf(stderr, "Memory allocation failed\n"); return 1; }
        for (int i = 0; i < q; i++) {
            w[i] = (float)((rand() % 100) + 1);
        }

        // Compute weight_sum = sum of w[i]
        float weight_sum = 0.0f;
        for (int i = 0; i < q; i++) {
            weight_sum += w[i];
        }

        // Allocate and zero-initialize the result vector p (n columns)
        float *result_p = (float*)malloc(n * sizeof(float));
        if (!result_p) {
            fprintf(stderr, "Memory allocation failed\n");
            return 1;
        }
        for (int i = 0; i < n; i++) {
            result_p[i] = 0.0f;
        }

        printf("[Master] Generated %dx%d matrix, q=%d, weight_sum=%.2f\n",
               n, n, q, weight_sum);

        // Read configuration file
        FILE *file = fopen("config.txt", "r");
        if (!file) {
            fprintf(stderr, "[Master] Error: Cannot open config.txt\n");
            return 1;
        }

        // Read master's own IP and port
        addr *master_address = malloc(sizeof(addr));
        master_address->ip = malloc(16);
        if (fscanf(file, " MASTER %15s %d", master_address->ip, &master_address->port) != 2) {
            fprintf(stderr, "[Master] Failed to read MASTER config\n");
            return 1;
        }
        printf("[Master] Self — %s:%d\n", master_address->ip, master_address->port);

        // Read slave count
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

        // Distribute submatrices to slaves and collect partial p vectors
        // master() handles spawning threads, timing, and joining
        master(n, num_slaves, slave_addresses, X, q, weight_sum, w, result_p);

        // Print assembled result vector p (first 10 entries max)
        printf("\n[Master] Result vector p (first %d entries):\n", (n < 10 ? n : 10));
        for (int j = 0; j < n && j < 10; j++) {
            printf("  p[%d] = %.6f\n", j, result_p[j]);
        }

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
        free(w);
        free(result_p);
    }

    // SLAVE BRANCH 
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

        fclose(file);

        // Wait for master, receive submatrix + WMA params, compute, return p
        slave(n, p);

    } else {
        fprintf(stderr, "Invalid status s=%d. Use 0 (master) or 1 (slave).\n", s);
        return 1;
    }

    return 0;
}