/*
 * lab05.c — Recursive Cascading 1MPB + M1PR Distributed WMA
 *
 * =============================================================================
 * ARCHITECTURE: RECURSIVE CASCADING TREE
 * =============================================================================
 *
 * Instead of a flat master→[slave0..slaveN] topology, nodes are arranged in
 * a tree. Each non-leaf node acts as BOTH a slave (receives from its parent)
 * AND a mini-master (distributes to its children):
 *
 *                        [Root / Master]
 *                       /               \
 *               [Node A]               [Node B]
 *               /      \               /      \
 *          [Leaf A0] [Leaf A1]   [Leaf B0] [Leaf B1]
 *
 * 1MPB (1-to-Many Personalized Broadcast):
 *   Each node receives a submatrix from its parent, keeps its own slice,
 *   and forwards DIFFERENT sub-slices to each of its children.
 *
 * M1PR (Many-to-1 Point Return):
 *   After computing on its own slice and collecting all children results,
 *   each node sends its assembled partial p back up to its parent.
 *
 * CASCADING:
 *   Broadcast cascades DOWN the tree (root → leaves).
 *   Result collection cascades UP the tree (leaves → root).
 *   Each intermediate node waits for all children before returning.
 *
 * =============================================================================
 * TWO COMPUTATION MODES
 * =============================================================================
 *
 * MODE 0 (Normal / Row-wise):
 *   Master splits X by ROWS. Each node receives rows [start..end] of X.
 *   WMA slides column-by-column across the received rows.
 *   p[j] = RMSE over those rows for column j.
 *
 * MODE 1 (Transposed / Column-wise):
 *   Master transposes X → XT, then splits XT by ROWS (= original columns).
 *   Each node receives a set of original columns packed as XT rows.
 *   WMA slides along the original column direction (XT row direction).
 *   p[col] = RMSE for the col-th assigned original column.
 *   Advantage: original columns are contiguous in memory → better cache.
 *
 * =============================================================================
 * CONFIG FILE FORMAT  (config.txt)
 * =============================================================================
 *
 *   MODE <0|1>
 *   NODE <id> <ip> <port> <parent_id> <num_children> [child_id ...]
 *   ...
 *
 * Example — 7-node balanced binary tree, root=0:
 *   MODE 0
 *   NODE 0 127.0.0.1 9000 -1 2 1 2
 *   NODE 1 127.0.0.1 9001  0 2 3 4
 *   NODE 2 127.0.0.1 9002  0 2 5 6
 *   NODE 3 127.0.0.1 9003  1 0
 *   NODE 4 127.0.0.1 9004  1 0
 *   NODE 5 127.0.0.1 9005  2 0
 *   NODE 6 127.0.0.1 9006  2 0
 *
 * parent_id = -1 means root (no parent).
 *
 * =============================================================================
 * PROTOCOL SEQUENCE (per parent → child TCP connection)
 * =============================================================================
 *
 *   Parent → Child : [assigned][n][submatrix rows][q][weight_sum][w[q]]
 *   Child  → Parent: [ack][p_child[assigned]]
 *
 * =============================================================================
 * COMPILATION
 * =============================================================================
 *
 *   gcc -o lab05 lab05.c -lpthread -lm
 *
 * =============================================================================
 * USAGE — start LEAVES first, then intermediates, then root LAST
 * =============================================================================
 *
 *   ./lab05
 *   Enter n (matrix size): <n>
 *   Enter p (my port):     <port>
 *   Enter id (my node id): <id>
 */

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


// =============================================================================
// DATA STRUCTURES
// =============================================================================

// One network endpoint (IP + port)
typedef struct ADDR {
    char ip[64];
    int  port;
} addr;

// A single node in the tree topology
typedef struct NODE {
    int  id;
    addr endpoint;       // This node's own reachable address
    int  parent_id;      // -1 if root
    int  num_children;
    int *child_ids;      // Array of child node IDs (size = num_children)
} node_t;

// Arguments for each child-dispatch thread (spawned by non-leaf nodes)
typedef struct CHILD_ARGS {
    addr   child_endpoint;  // Child's IP and port to connect to
    int    assigned;        // Number of rows/cols assigned to this child
    int    start;           // Offset within the parent's received slice
    int    n;               // Width (number of columns of X, or n for mode 1)
    int    q;
    float  weight_sum;
    float *w;               // Weight vector (shared read-only)
    float **local_matrix;   // The full slice this node received (shared read-only)
    float *p_child;         // OUTPUT: filled after receiving child result (size=assigned)
} child_args_t;


// =============================================================================
// GLOBAL TOPOLOGY (parsed from config.txt once at startup)
// =============================================================================

static int     g_mode      = 0;    // 0 = normal row-wise, 1 = transposed col-wise
static int     g_num_nodes = 0;
static node_t *g_nodes     = NULL;

// Look up a node by its integer ID
node_t *find_node(int id) {
    for (int i = 0; i < g_num_nodes; i++)
        if (g_nodes[i].id == id) return &g_nodes[i];
    return NULL;
}


// =============================================================================
// RELIABLE SEND / RECEIVE
// =============================================================================

// recv_all -> Receive exactly `size` bytes; blocks until done or fatal error
void recv_all(int sock, void *buffer, size_t size) {
    size_t total = 0;
    while (total < size) {
        int r = recv(sock, (char*)buffer + total, size - total, 0);
        if (r <= 0) { perror("recv_all"); exit(1); }
        total += r;
    }
}

// send_all -> Send exactly `size` bytes; blocks until done or fatal error
void send_all(int sock, void *buffer, size_t size) {
    size_t total = 0;
    while (total < size) {
        int s = send(sock, (char*)buffer + total, size - total, 0);
        if (s <= 0) { perror("send_all"); exit(1); }
        total += s;
    }
}


// =============================================================================
// MATRIX TRANSPOSE
// =============================================================================

// Returns a newly allocated n×n matrix that is the transpose of X.
// Caller frees: for(i) free(XT[i]); free(XT);
float **transpose_matrix(float **X, int n) {
    float **XT = malloc(sizeof(float*) * n);
    if (!XT) { perror("transpose: malloc"); exit(1); }
    for (int i = 0; i < n; i++) {
        XT[i] = malloc(sizeof(float) * n);
        if (!XT[i]) { perror("transpose: malloc row"); exit(1); }
    }
    for (int i = 0; i < n; i++)
        for (int j = 0; j < n; j++)
            XT[i][j] = X[j][i];
    return XT;
}


// =============================================================================
// WMA + RMSE: ROW-WISE SLIDING WINDOW  (Mode 0)
// =============================================================================
/*
 * mse_wma_rowwise — Compute RMSE of WMA error for each column of a submatrix.
 *
 * submatrix[m][n_cols] — row slice assigned to this node.
 * For each column j, slides an order-q WMA window across rows 0..m-1.
 *
 * SLIDING WINDOW (advancing row i → i+1, fixed column j):
 *   weighted(i)   = Σ_{k=0}^{q-1} w[k] * submatrix[i-q+k][j]
 *   weighted(i+1) = weighted(i)
 *                   - w[0]   * submatrix[i-q][j]       (drop oldest)
 *                   + w[q-1] * submatrix[i][j]          (add newest)
 *                   + diff_sum                           (weight re-alignment)
 *   diff_sum = Σ_{l=0}^{q-2} (w[l]-w[l+1]) * submatrix[i-q+l+1][j]
 *   diff_sum slides O(1) per step using precomputed weight_diff[].
 *
 * Complexity: O(m × n_cols) — sliding window eliminates the inner q loop.
 * Output: p_out[j] = sqrt(Σ errors²) / (m - q)
 */
void mse_wma_rowwise(float **submatrix, int m, int n_cols,
                     int q, float weight_sum, float *w, float *p_out) {
    if (m <= q) {
        fprintf(stderr, "[WMA-row] m(%d) <= q(%d): no WMA computable.\n", m, q);
        for (int j = 0; j < n_cols; j++) p_out[j] = 0.0f;
        return;
    }

    // weight_diff[l] = w[l] - w[l+1] for l in [0, q-2]
    float *weight_diff = malloc(sizeof(float) * (q - 1));
    if (!weight_diff) { perror("malloc weight_diff"); exit(1); }
    for (int l = 0; l < q - 1; l++)
        weight_diff[l] = w[l] - w[l + 1];

    for (int j = 0; j < n_cols; j++) {

        // Initialize for i = q (first row where WMA is defined)
        float weighted = 0.0f, diff_sum = 0.0f, mse_sum = 0.0f;

        for (int k = 0; k < q; k++)
            weighted += w[k] * submatrix[k][j];
        for (int k = 0; k < q - 1; k++)
            diff_sum += weight_diff[k] * submatrix[k + 1][j];

        // Slide row-by-row, O(1) update per step
        for (int i = q; i < m; i++) {
            float wma  = weighted / weight_sum;
            float diff = submatrix[i][j] - wma;
            mse_sum   += diff * diff;

            if (i + 1 < m && q > 1) {
                diff_sum += weight_diff[q - 2] * submatrix[i][j];
                diff_sum -= weight_diff[0]     * submatrix[i - q + 1][j];
                weighted -= w[0]     * submatrix[i - q][j];
                weighted += w[q - 1] * submatrix[i][j];
                weighted += diff_sum;
            }
        }

        p_out[j] = sqrtf(mse_sum) / (float)(m - q);
    }

    free(weight_diff);
}


// =============================================================================
// WMA + RMSE: TRANSPOSED SLIDING WINDOW  (Mode 1)
// =============================================================================
/*
 * mse_wma_transposed — Each row of XT_slice is one ORIGINAL column of X.
 * XT_slice[col][i] = X[i][original_col]
 *
 * Sliding window runs along dimension i (= original row index), which is
 * now the second index of XT_slice — contiguous in memory → cache-friendly.
 *
 * Same derivation as rowwise, axes swapped:
 *   outer: col ∈ [0, assigned_cols)
 *   inner: i   ∈ [q, n)
 *
 * Output: p_out[col] = RMSE for the col-th assigned original column.
 */
void mse_wma_transposed(float **XT_slice, int assigned_cols, int n,
                        int q, float weight_sum, float *w, float *p_out) {
    if (n <= q) {
        fprintf(stderr, "[WMA-T] n(%d) <= q(%d): no WMA computable.\n", n, q);
        for (int col = 0; col < assigned_cols; col++) p_out[col] = 0.0f;
        return;
    }

    float *weight_diff = malloc(sizeof(float) * (q - 1));
    if (!weight_diff) { perror("malloc weight_diff"); exit(1); }
    for (int k = 0; k < q - 1; k++)
        weight_diff[k] = w[k] - w[k + 1];

    for (int col = 0; col < assigned_cols; col++) {

        float weighted = 0.0f, diff_sum = 0.0f, mse_sum = 0.0f;

        for (int k = 0; k < q; k++)
            weighted += w[k] * XT_slice[col][k];
        for (int k = 0; k < q - 1; k++)
            diff_sum += weight_diff[k] * XT_slice[col][k + 1];

        for (int i = q; i < n; i++) {
            float wma  = weighted / weight_sum;
            float diff = XT_slice[col][i] - wma;
            mse_sum   += diff * diff;

            if (i + 1 < n && q > 1) {
                diff_sum += weight_diff[q - 2] * XT_slice[col][i];
                diff_sum -= weight_diff[0]     * XT_slice[col][i - q + 1];
                weighted -= w[0]     * XT_slice[col][i - q];
                weighted += w[q - 1] * XT_slice[col][i];
                weighted += diff_sum;
            }
        }

        p_out[col] = sqrtf(mse_sum) / (float)(n - q);
    }

    free(weight_diff);
}


// =============================================================================
// CHILD DISPATCH THREAD  (the downward 1MPB leg)
// =============================================================================
/*
 * dispatch_child — Thread function. Connects to one child, sends its sub-slice,
 * waits for ACK, then receives the child's assembled result p_child[].
 *
 * 1MPB: sends matrix[start .. start+assigned-1] rows to child.
 * M1PR: blocks until child sends back p_child[assigned] floats.
 *
 * The child may itself be a non-leaf and will further cascade — the parent
 * thread just blocks on the socket and doesn't care about the child's internals.
 */
void *dispatch_child(void *varg) {
    child_args_t *a = (child_args_t *)varg;

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) { perror("dispatch_child: socket"); pthread_exit(NULL); }

    struct sockaddr_in caddr = {0};
    caddr.sin_family = AF_INET;
    caddr.sin_port   = htons(a->child_endpoint.port);
    if (inet_pton(AF_INET, a->child_endpoint.ip, &caddr.sin_addr) <= 0) {
        perror("dispatch_child: inet_pton"); close(sock); pthread_exit(NULL);
    }
    if (connect(sock, (struct sockaddr*)&caddr, sizeof(caddr)) < 0) {
        perror("dispatch_child: connect"); close(sock); pthread_exit(NULL);
    }

    printf("[Dispatch] Connected to child %s:%d (assigned=%d, start=%d)\n",
           a->child_endpoint.ip, a->child_endpoint.port, a->assigned, a->start);

    // --- 1MPB: Send child's slice ---
    int net_assigned = htonl(a->assigned);
    send_all(sock, &net_assigned, sizeof(int));

    int net_n = htonl(a->n);
    send_all(sock, &net_n, sizeof(int));

    // Send rows [start .. start+assigned-1] of local_matrix
    for (int r = 0; r < a->assigned; r++)
        send_all(sock, a->local_matrix[a->start + r], sizeof(float) * a->n);

    // Send WMA parameters
    int net_q = htonl(a->q);
    send_all(sock, &net_q, sizeof(int));
    send_all(sock, &a->weight_sum, sizeof(float));
    send_all(sock, a->w, sizeof(float) * a->q);

    // --- M1PR: Receive ACK then result ---
    char ack[4] = {0};
    recv_all(sock, ack, 3);
    ack[3] = '\0';
    if (strncmp(ack, "ack", 3) != 0)
        fprintf(stderr, "[Dispatch] WARNING: bad ACK from %s:%d\n",
                a->child_endpoint.ip, a->child_endpoint.port);

    // Allocate and receive the child's partial result
    a->p_child = malloc(sizeof(float) * a->assigned);
    if (!a->p_child) { perror("dispatch_child: malloc p_child"); pthread_exit(NULL); }
    recv_all(sock, a->p_child, sizeof(float) * a->assigned);

    printf("[Dispatch] Received result from child %s:%d\n",
           a->child_endpoint.ip, a->child_endpoint.port);

    close(sock);
    pthread_exit(NULL);
}


// =============================================================================
// PROCESS NODE  (core of the cascade: split → dispatch → compute → assemble)
// =============================================================================
/*
 * process_node — Called once this node has its assigned slice ready.
 *
 * SPLIT STRATEGY:
 *   Participants = num_children + 1 (self always takes the last slice).
 *   Base = assigned / participants.
 *   First (assigned % participants) get one extra row/col.
 *
 *   Children indices 0..num_children-1 are dispatched first (in parallel).
 *   Self computes its slice while waiting for children to finish.
 *
 * ASSEMBLE:
 *   p_out is filled in order: child_0's result, child_1's result, ..., self's result.
 *   This preserves the original row/col ordering of the assigned slice.
 *
 * Parameters:
 *   matrix    — the assigned slice (assigned × n), rows indexed 0..assigned-1
 *   assigned  — number of rows (mode 0) or original columns (mode 1) this node owns
 *   n         — width of each row (= n_cols for mode 0, = n for mode 1)
 *   my_node   — this node's config (to look up children)
 *   p_out     — output float[assigned], written by this function
 */
void process_node(float **matrix, int assigned, int n,
                  int q, float weight_sum, float *w,
                  node_t *my_node, float *p_out) {

    int num_children = my_node->num_children;
    int total        = num_children + 1;  // +1 for self

    int base      = assigned / total;
    int remainder = assigned % total;

    child_args_t *cargs   = malloc(sizeof(child_args_t) * num_children);
    pthread_t    *threads = malloc(sizeof(pthread_t)    * num_children);
    if (!cargs || !threads) { perror("process_node: malloc"); exit(1); }

    // Core affinity: dispatch threads go to cores 1+ (leave core 0 for self)
    int total_cores  = sysconf(_SC_NPROCESSORS_ONLN);
    int usable_cores = (total_cores > 1) ? total_cores - 1 : 1;

    int current = 0;  // offset within this node's slice

    // --- Spawn one dispatch thread per child (1MPB downward) ---
    for (int i = 0; i < num_children; i++) {
        int child_assigned = base + (i < remainder ? 1 : 0);

        node_t *child_node = find_node(my_node->child_ids[i]);
        if (!child_node) {
            fprintf(stderr, "[Node %d] ERROR: child id %d not in config\n",
                    my_node->id, my_node->child_ids[i]);
            exit(1);
        }

        cargs[i].child_endpoint = child_node->endpoint;
        cargs[i].assigned       = child_assigned;
        cargs[i].start          = current;
        cargs[i].n              = n;
        cargs[i].q              = q;
        cargs[i].weight_sum     = weight_sum;
        cargs[i].w              = w;
        cargs[i].local_matrix   = matrix;
        cargs[i].p_child        = NULL;

        // Pin each dispatch thread to a different core
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        int core = (i % usable_cores) + 1;
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(core, &cpuset);
        if (pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset) != 0)
            fprintf(stderr, "[Node %d] Warning: affinity failed for thread %d\n",
                    my_node->id, i);

        if (pthread_create(&threads[i], &attr, dispatch_child, &cargs[i]) != 0) {
            perror("process_node: pthread_create"); exit(1);
        }
        pthread_attr_destroy(&attr);
        current += child_assigned;
    }

    // --- Self computation (runs concurrently with child dispatch threads) ---
    // Self always takes the last slice so child start offsets are clean.
    int self_assigned = base + (num_children < remainder ? 1 : 0);
    int self_start    = current;

    float *self_p = malloc(sizeof(float) * self_assigned);
    if (!self_p) { perror("process_node: malloc self_p"); exit(1); }

    // &matrix[self_start] is a pointer into the existing rows — no copy needed
    float **self_slice = &matrix[self_start];

    if (g_mode == 0)
        mse_wma_rowwise(self_slice, self_assigned, n, q, weight_sum, w, self_p);
    else
        mse_wma_transposed(self_slice, self_assigned, n, q, weight_sum, w, self_p);

    printf("[Node %d] Local computation done (%d slices).\n",
           my_node->id, self_assigned);

    // --- Join all child dispatch threads (M1PR upward collection) ---
    for (int i = 0; i < num_children; i++)
        pthread_join(threads[i], NULL);

    // --- Assemble p_out in original order: children first, then self ---
    int out_offset = 0;
    for (int i = 0; i < num_children; i++) {
        memcpy(p_out + out_offset, cargs[i].p_child, sizeof(float) * cargs[i].assigned);
        out_offset += cargs[i].assigned;
        free(cargs[i].p_child);
    }
    memcpy(p_out + out_offset, self_p, sizeof(float) * self_assigned);

    free(self_p);
    free(cargs);
    free(threads);
}


// =============================================================================
// ROOT NODE ENTRY POINT
// =============================================================================
/*
 * run_root — Only the root (parent_id == -1) calls this.
 *
 * Generates X, computes WMA parameters, records time_before, calls
 * process_node to kick off the full cascading tree, records time_after.
 */
void run_root(int n, node_t *my_node) {
    // Allocate n×n matrix with random positive integers [1, 100]
    float **X = malloc(sizeof(float*) * n);
    if (!X) { perror("run_root: malloc X"); exit(1); }
    for (int i = 0; i < n; i++) {
        X[i] = malloc(sizeof(float) * n);
        if (!X[i]) { perror("run_root: malloc X row"); exit(1); }
        for (int j = 0; j < n; j++)
            X[i][j] = (float)((rand() % 100) + 1);
    }

    // q = max(S1*10 + S2, S1*S2), S1=9, S2=6 → max(96, 54) = 96
    int q = 96;

    // Weight vector w[q] with random positive floats [1, 100]
    float *w = malloc(sizeof(float) * q);
    if (!w) { perror("run_root: malloc w"); exit(1); }
    for (int i = 0; i < q; i++)
        w[i] = (float)((rand() % 100) + 1);

    // weight_sum = Σ w[i]
    float weight_sum = 0.0f;
    for (int i = 0; i < q; i++)
        weight_sum += w[i];

    // Result vector p[n]: one RMSE value per column (mode 0) or original column (mode 1)
    float *result_p = calloc(n, sizeof(float));
    if (!result_p) { perror("run_root: calloc result_p"); exit(1); }

    printf("[Root] n=%d q=%d weight_sum=%.2f mode=%s\n",
           n, q, weight_sum, g_mode == 0 ? "row-wise" : "transposed");

    // Mode 1: transpose X so each "row" of work_matrix is one original column
    float **work_matrix = (g_mode == 1) ? transpose_matrix(X, n) : X;

    struct timespec t_before, t_after;

    // time_before: just before starting the broadcast cascade
    clock_gettime(CLOCK_MONOTONIC, &t_before);

    // Launch the recursive cascading 1MPB + M1PR over the full tree
    process_node(work_matrix, n, n, q, weight_sum, w, my_node, result_p);

    // time_after: after root has fully assembled result_p from all descendants
    clock_gettime(CLOCK_MONOTONIC, &t_after);

    double elapsed =
        (t_after.tv_sec  - t_before.tv_sec) +
        (t_after.tv_nsec - t_before.tv_nsec) / 1e9;

    printf("\n[Root] Cascade complete.\n");
    printf("[Root] time_elapsed = %.9f seconds\n", elapsed);

    // Print first min(n, 10) results
    int show = (n < 10) ? n : 10;
    printf("[Root] Result vector p (first %d entries):\n", show);
    for (int j = 0; j < show; j++)
        printf("  p[%d] = %.6f\n", j, result_p[j]);

    // Cleanup
    for (int i = 0; i < n; i++) free(X[i]);
    free(X);
    if (g_mode == 1) {
        for (int i = 0; i < n; i++) free(work_matrix[i]);
        free(work_matrix);
    }
    free(w);
    free(result_p);
}


// =============================================================================
// NON-ROOT NODE ENTRY POINT  (slave + optional mini-master)
// =============================================================================
/*
 * run_node — Called by every node that is NOT the root.
 *
 * Receives slice from parent → sends ACK → calls process_node
 * (which may cascade further) → sends result back up.
 *
 * TIMING (per spec):
 *   time_before: AFTER receiving all data, BEFORE computation starts.
 *   time_after:  AFTER process_node returns (own compute + all children done).
 *   Reports time_elapsed = pure computation time of this node's subtree.
 *   Communication with parent (receive + send) is NOT counted.
 */
void run_node(int n, int port, node_t *my_node) {
    // n is passed for context (e.g. future verification); actual dimensions
    // come from the parent via the network protocol.
    (void)n;
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("run_node: socket"); exit(1); }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in saddr = {0};
    saddr.sin_family      = AF_INET;
    saddr.sin_port        = htons(port);
    saddr.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_fd, (struct sockaddr*)&saddr, sizeof(saddr)) < 0) {
        perror("run_node: bind"); close(server_fd); exit(1);
    }
    if (listen(server_fd, 5) < 0) {
        perror("run_node: listen"); close(server_fd); exit(1);
    }

    printf("[Node %d] Listening on port %d...\n", my_node->id, port);

    // Accept exactly one connection (from parent)
    int sock = accept(server_fd, NULL, NULL);
    if (sock < 0) { perror("run_node: accept"); exit(1); }

    printf("[Node %d] Parent connected.\n", my_node->id);

    // --- Receive slice metadata ---
    int net_assigned, net_n;
    recv_all(sock, &net_assigned, sizeof(int));
    int assigned = ntohl(net_assigned);

    recv_all(sock, &net_n, sizeof(int));
    int n_cols = ntohl(net_n);

    // --- Receive submatrix rows ---
    float **submatrix = malloc(assigned * sizeof(float*));
    if (!submatrix) { perror("run_node: malloc submatrix"); exit(1); }
    for (int i = 0; i < assigned; i++) {
        submatrix[i] = malloc(n_cols * sizeof(float));
        if (!submatrix[i]) { perror("run_node: malloc row"); exit(1); }
        recv_all(sock, submatrix[i], sizeof(float) * n_cols);
    }

    // --- Receive WMA parameters ---
    int net_q;
    recv_all(sock, &net_q, sizeof(int));
    int q = ntohl(net_q);

    float weight_sum;
    recv_all(sock, &weight_sum, sizeof(float));

    float *w = malloc(q * sizeof(float));
    if (!w) { perror("run_node: malloc w"); exit(1); }
    recv_all(sock, w, sizeof(float) * q);

    printf("[Node %d] Received slice: %d rows x %d cols, q=%d\n",
           my_node->id, assigned, n_cols, q);

    // Submatrix verification (small matrices only)
    if (assigned <= 6 && n_cols <= 6) {
        printf("[Node %d] Slice:\n", my_node->id);
        for (int i = 0; i < assigned; i++) {
            printf("  Row %d:", i);
            for (int j = 0; j < n_cols; j++) printf(" %6.1f", submatrix[i][j]);
            printf("\n");
        }
    } else {
        printf("[Node %d] submatrix[0][0]=%.1f, submatrix[%d][%d]=%.1f\n",
               my_node->id, submatrix[0][0],
               assigned - 1, n_cols - 1, submatrix[assigned-1][n_cols-1]);
    }

    // --- Send ACK to parent BEFORE starting computation ---
    // The parent's dispatch_child thread unblocks from recv-ack and immediately
    // blocks on recv(p_child). This node then does its work concurrently.
    send_all(sock, "ack", 3);

    // Allocate result vector
    float *p_out = calloc(assigned, sizeof(float));
    if (!p_out) { perror("run_node: calloc p_out"); exit(1); }

    struct timespec t_before, t_after;

    // time_before: AFTER receiving, BEFORE computation (spec requirement)
    clock_gettime(CLOCK_MONOTONIC, &t_before);

    // Cascade: compute own slice + dispatch to children + collect
    process_node(submatrix, assigned, n_cols, q, weight_sum, w, my_node, p_out);

    // time_after: AFTER own compute + all children returned (spec requirement)
    clock_gettime(CLOCK_MONOTONIC, &t_after);

    double elapsed =
        (t_after.tv_sec  - t_before.tv_sec) +
        (t_after.tv_nsec - t_before.tv_nsec) / 1e9;

    printf("[Node %d] time_elapsed (subtree computation) = %.9f seconds\n",
           my_node->id, elapsed);

    // --- M1PR: Send result back to parent ---
    send_all(sock, p_out, sizeof(float) * assigned);

    printf("[Node %d] Result sent to parent.\n", my_node->id);

    // Cleanup
    for (int i = 0; i < assigned; i++) free(submatrix[i]);
    free(submatrix);
    free(w);
    free(p_out);

    close(sock);
    close(server_fd);
}


// =============================================================================
// CONFIG READER
// =============================================================================
/*
 * read_config — Parses config.txt into g_mode, g_num_nodes, g_nodes[].
 *
 * Line formats:
 *   MODE <0|1>
 *   NODE <id> <ip> <port> <parent_id> <num_children> [child_id ...]
 */
void read_config(const char *filename) {
    FILE *f = fopen(filename, "r");
    if (!f) { perror("Cannot open config.txt"); exit(1); }

    // First pass: count NODE lines
    char line[512];
    g_num_nodes = 0;
    while (fgets(line, sizeof(line), f))
        if (strncmp(line, "NODE", 4) == 0) g_num_nodes++;
    rewind(f);

    g_nodes = malloc(sizeof(node_t) * g_num_nodes);
    if (!g_nodes) { perror("malloc g_nodes"); exit(1); }

    int idx = 0;
    while (fgets(line, sizeof(line), f)) {
        if (strncmp(line, "MODE", 4) == 0) {
            sscanf(line, "MODE %d", &g_mode);
        } else if (strncmp(line, "NODE", 4) == 0) {
            node_t *nd = &g_nodes[idx++];
            nd->child_ids = NULL;

            // Tokenize: NODE <id> <ip> <port> <parent_id> <num_children> [child_ids...]
            strtok(line, " \t\n");                            // skip "NODE"
            nd->id              = atoi(strtok(NULL, " \t\n"));
            strncpy(nd->endpoint.ip, strtok(NULL, " \t\n"), 63);
            nd->endpoint.port   = atoi(strtok(NULL, " \t\n"));
            nd->parent_id       = atoi(strtok(NULL, " \t\n"));
            nd->num_children    = atoi(strtok(NULL, " \t\n"));

            if (nd->num_children > 0) {
                nd->child_ids = malloc(sizeof(int) * nd->num_children);
                if (!nd->child_ids) { perror("malloc child_ids"); exit(1); }
                for (int c = 0; c < nd->num_children; c++)
                    nd->child_ids[c] = atoi(strtok(NULL, " \t\n"));
            }
        }
    }

    fclose(f);

    printf("[Config] mode=%d nodes=%d\n", g_mode, g_num_nodes);
    for (int i = 0; i < g_num_nodes; i++) {
        node_t *nd = &g_nodes[i];
        printf("  Node %d  %s:%d  parent=%d  children=%d",
               nd->id, nd->endpoint.ip, nd->endpoint.port,
               nd->parent_id, nd->num_children);
        for (int c = 0; c < nd->num_children; c++)
            printf(" [%d]", nd->child_ids[c]);
        printf("\n");
    }
}


// =============================================================================
// MAIN
// =============================================================================

int main() {
    int n, port, my_id;

    // Each instance identifies itself by its node id from config.txt
    printf("Enter size of square matrix n: ");
    scanf("%d", &n);
    printf("Enter my port number p: ");
    scanf("%d", &port);
    printf("Enter my node id: ");
    scanf("%d", &my_id);

    // Seed RNG differently per node so matrix values aren't identical
    srand((unsigned int)time(NULL) ^ (unsigned int)my_id);

    // Parse topology from config.txt
    read_config("final_config.txt");

    node_t *my_node = find_node(my_id);
    if (!my_node) {
        fprintf(stderr, "Node id %d not found in config.txt\n", my_id);
        return 1;
    }

    printf("[Main] Node %d  %s:%d  parent=%d  children=%d  mode=%s\n",
           my_node->id, my_node->endpoint.ip, my_node->endpoint.port,
           my_node->parent_id, my_node->num_children,
           g_mode == 0 ? "row-wise" : "transposed");

    if (my_node->parent_id == -1)
        run_root(n, my_node);     // Root: generate data, start cascade, report total time
    else
        run_node(n, port, my_node); // Non-root: receive, cascade, compute, return, report subtree time

    // Global config cleanup
    for (int i = 0; i < g_num_nodes; i++)
        if (g_nodes[i].child_ids) free(g_nodes[i].child_ids);
    free(g_nodes);

    return 0;
}