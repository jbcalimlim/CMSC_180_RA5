#define _POSIX_C_SOURCE 199309L // for clock_gettime and CLOCK_MONOTONIC
#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <time.h>


// Computes for the MSE
// mxn Square Matrix X; vector w; result vector p; int q, int m, int n 
void mse_wma(float** X, float* w, float* p, int q, float m, float n, float weight_sum){
    // j - column, i - row
    if (m <= q) return; // protection against division by zero

    // Precompute the weight difference ahead of time for easier computation
    // later when doing sliding window so the weight difference will be the basis
    // instead of recomputing the weighted sums again
    // O(n)
    float *weight_diff = (float*)malloc(sizeof(float)*(q-1));
    for (int l = 0; l < q - 1; l++){
        weight_diff[l] = w[l] - w[l+1];
    }
    
    // O(n^2)
    for(int j = 0; j < n; j++){
        // window data
        // weighted sum: sum of elements * respective weights
        // weighted difference sum: sum of weighted difference * respective elements
        // used for adjusting the weighted sum
        // ----------------------------------
        float weighted = 0; // weighted sum
        float diff_sum = 0; // summation of weighted difference
        // ----------------------------------
        float sum_mse = 0; //  final mean square error

        // Computes for the WMA
        // Initialized WMA for i = q
        // O(q)
        for (int k = 0; k < q; k++){
            weighted  += (w[k] * X[k][j]);
        }

        // Computes the summation of difference of weights
        // O(q)
        for (int k = 0; k < q - 1; k++){
            diff_sum += weight_diff[k] * X[k+1][j];
        }

        // Perform sliding window to optimize calculation
        // Sliding window is where the oldest window data is removed and the newest
        // window data is added

        // O(n)
        for(int i = q; i < m; i++){
            float wma_result = weighted/weight_sum; 
            float sum_difference = X[i][j] - wma_result; 
            sum_mse += sum_difference * sum_difference;

            // if row is changed, do sliding window 
            //O(1)
            // Checks if q > 1 to ensure that only valid indices will be used for accessing weight_diff
            if (i + 1 < m && q > 1) {

                // Update diff_sum first
                // compute and add newest diff_sum data
                diff_sum += weight_diff[q - 2] * X[i][j];
                // remove oldest diff_sum data
                diff_sum -= weight_diff[0] * X[i-q+1][j];

                // Update weighted sum
                // Remove the oldest value
                weighted -= w[0] * X[i - q][j];
                // Add the new value
                weighted += w[q - 1] * X[i][j];
                // Add the weighted shift
                weighted += diff_sum;

            }
        }

        // Compute the MSE given the WMA
        // O(1)
        float a = sqrt(sum_mse);
        p[j] = a/(m - q);
    }

    free(weight_diff);
}


int main() {

    int n, q;
    float a, b, b1, b2;
    srand(time(NULL)); // Initialize the rng and using time as basis
    printf("Enter n: ");
    scanf("%d", &n);
    // 96310 S_1 = 9, S_2 = 6

    // q = max(a, b)
    // S_1 = 9, S_2 = 6
    int s1 = 9, s2 = 6;
    a = s1 * 10 + s2;
    b = s1 * s2;
    q = a > b? a : b;
    // Initialize square matrix X 
    float **X = (float**)malloc(sizeof(float*)*n);
    if (X == NULL) {
        printf("Memory allocation failed");
        return 1;
    }

    // populate with random integer from 1 to 100
    for(int i = 0; i < n; i++){
        float *row = (float*)malloc(sizeof(float)*n);
        for (int j = 0; j < n; j++) {
            int random_int = (rand() % (100 - 1 + 1)) + 1;
            row[j] = random_int;
        }
        X[i] = row;
    }

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

    // Create an n * 1 vector p
    float *p = (float*)malloc(sizeof(float)*n);
    if (p == NULL) {
        printf("Memory allocation failed");
        return 1;
    }

    // Sum of weights computation ahead of time 
    // O(n)
    float weight_sum = 0;
    for (int l = 0; l < q; l++){
        weight_sum += w[l];
    }

    // Initialized the start and end time to get the elapsed time
    struct timespec start_time, end_time;
    float elapsed;

    // Retrieve the real clock time of the system to both start and end time
    clock_gettime(CLOCK_MONOTONIC, &start_time);

    // Function call to perform the mse and wma calculation
    mse_wma(X, w, p, q, n, n, weight_sum);

    clock_gettime(CLOCK_MONOTONIC, &end_time);

    // Get the elapsed time converted into seconds
    elapsed = (end_time.tv_sec - start_time.tv_sec) + (end_time.tv_nsec - start_time.tv_nsec) /1e9;

    // Print the resulting time
    printf("time elapsed: %.9f seconds\n", elapsed);

    // Cleanup: Free allocated memory :) 
    if (X) {
            for (int i = 0; i < n; i++) {
                free(X[i]);
            }
            free(X);
        }
        free(w);
        free(p);

        return 0;
}