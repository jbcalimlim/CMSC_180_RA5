#!/bin/bash
# ============================================================
#  Cluster Orchestrator — WMA/MSE Benchmark
#  Runs t = 2, 4, 8, 16 slaves × 3 iterations each
#  Records master time, all slave times, and max slave time
# ============================================================

# ===== USER CONFIG =====
REMOTE_USER="jgcalimlim"
PROGRAM_PATH="/home/jgcalimlim/main"
N=4000
MASTER_IP="10.0.9.136"

# All 16 slaves with their fixed ports
ALL_SLAVES=(
    "10.0.9.138 3001"
    "10.0.9.133 3002"
    "10.0.9.163 3003"
    "10.0.9.175 3004"
    "10.0.9.180 3005"
    "10.0.9.13  3006"
    "10.0.9.131 3007"
    "10.0.9.166 3008"
    "10.0.9.173 3009"
    "10.0.9.139 3010"
    "10.0.9.171 3011"
    "10.0.9.124 3012"
    "10.0.9.135 3013"
    "10.0.9.184 3014"
    "10.0.9.123 3015"
    "10.0.9.162 3016"
)

RESULTS_FILE="benchmark_results.txt"
SLAVE_COUNTS=(2 4 8 16)
ITERATIONS=3
# =======================

# ---- One-time SSH password setup via ssh-copy-id ----
setup_ssh_keys() {
    echo "=============================================="
    echo " SSH Key Setup (one-time password entry)"
    echo "=============================================="

    # Collect all unique IPs
    declare -A SEEN
    ALL_IPS=("$MASTER_IP")
    SEEN["$MASTER_IP"]=1

    for entry in "${ALL_SLAVES[@]}"; do
        read -r IP PORT <<< "$entry"
        IP_TRIMMED=$(echo "$IP" | tr -d ' ')
        if [[ -z "${SEEN[$IP_TRIMMED]}" ]]; then
            ALL_IPS+=("$IP_TRIMMED")
            SEEN["$IP_TRIMMED"]=1
        fi
    done

    # Generate key if missing
    if [ ! -f "$HOME/.ssh/id_rsa" ]; then
        echo "Generating SSH key pair..."
        ssh-keygen -t rsa -b 2048 -N "" -f "$HOME/.ssh/id_rsa" -q
    fi

    echo "You will be prompted for the password for each host."
    echo "After this step, no more passwords will be needed."
    echo ""

    for IP in "${ALL_IPS[@]}"; do
        echo "  → Copying key to $REMOTE_USER@$IP ..."
        ssh-copy-id -o StrictHostKeyChecking=no "$REMOTE_USER@$IP" 2>/dev/null
        if [ $? -ne 0 ]; then
            echo "  [WARN] ssh-copy-id to $IP may have failed. Continuing..."
        fi
    done

    echo ""
    echo "SSH key setup complete. Running benchmark..."
    echo "=============================================="
    echo ""
}

# ---- Cleanup helper ----
cleanup() {
    local NUM_SLAVES=$1
    shift
    local SLAVES=("$@")

    ssh -o StrictHostKeyChecking=no -o BatchMode=yes \
        "$REMOTE_USER@$MASTER_IP" "killall main 2>/dev/null" &

    for entry in "${SLAVES[@]}"; do
        read -r IP PORT <<< "$entry"
        IP=$(echo "$IP" | tr -d ' ')
        ssh -n -o StrictHostKeyChecking=no -o BatchMode=yes \
            "$REMOTE_USER@$IP" "killall main 2>/dev/null" &
    done
    wait
}

# ---- Run one benchmark iteration ----
# Usage: run_iteration <t> <iteration_num> <slave_array_entries...>
run_iteration() {
    local T=$1
    local ITER=$2
    shift 2
    local SLAVES=("$@")

    echo ""
    echo "  [t=$T | Iter $ITER] Launching $T slaves..."

    # Arrays to hold slave log files
    declare -a SLAVE_LOG_FILES

    # Launch slaves in background; capture stdout to temp log
    for entry in "${SLAVES[@]}"; do
        read -r IP PORT <<< "$entry"
        IP=$(echo "$IP" | tr -d ' ')

        SLAVE_LOG=$(mktemp /tmp/slave_${IP}_${PORT}_XXXXXX.log)
        SLAVE_LOG_FILES+=("$SLAVE_LOG $IP $PORT")

        ssh -n -o StrictHostKeyChecking=no -o BatchMode=yes "$REMOTE_USER@$IP" \
            "cd /home/$REMOTE_USER && echo -e '$N\n$PORT\n1' | $PROGRAM_PATH" \
            > "$SLAVE_LOG" 2>&1 &
    done

    echo "  [t=$T | Iter $ITER] Waiting 10s for slaves to initialise..."
    sleep 10

    # Launch master; capture stdout
    MASTER_LOG=$(mktemp /tmp/master_XXXXXX.log)
    echo "  [t=$T | Iter $ITER] Launching master on $MASTER_IP..."
    ssh -o StrictHostKeyChecking=no -o BatchMode=yes "$REMOTE_USER@$MASTER_IP" \
        "cd /home/$REMOTE_USER && echo -e '$N\n5000\n0' | $PROGRAM_PATH" \
        > "$MASTER_LOG" 2>&1

    # ---- Parse master time ----
    MASTER_TIME=$(grep -oP 'time_elapsed\s*=\s*\K[0-9]+\.[0-9]+' "$MASTER_LOG" | head -1)
    MASTER_TIME=${MASTER_TIME:-"N/A"}

    # ---- Parse slave times ----
    MAX_SLAVE_TIME="0"
    SLAVE_SUMMARY=""

    for entry in "${SLAVE_LOG_FILES[@]}"; do
        read -r LOG_FILE IP PORT <<< "$entry"
        SLAVE_TIME=$(grep -oP 'time_elapsed.*?=\s*\K[0-9]+\.[0-9]+' "$LOG_FILE" | head -1)
        SLAVE_TIME=${SLAVE_TIME:-"N/A"}

        SLAVE_SUMMARY+="      Slave $IP:$PORT => ${SLAVE_TIME}s\n"

        # Track maximum (skip N/A)
        if [[ "$SLAVE_TIME" != "N/A" ]]; then
            if (( $(echo "$SLAVE_TIME > $MAX_SLAVE_TIME" | bc -l) )); then
                MAX_SLAVE_TIME=$SLAVE_TIME
            fi
        fi

        rm -f "$LOG_FILE"
    done

    [[ "$MAX_SLAVE_TIME" == "0" ]] && MAX_SLAVE_TIME="N/A"

    # ---- Print to console ----
    echo "  [t=$T | Iter $ITER] Master time     : ${MASTER_TIME}s"
    echo "  [t=$T | Iter $ITER] Max slave time  : ${MAX_SLAVE_TIME}s"

    # ---- Write to results file ----
    {
        echo "  Iteration $ITER:"
        echo "    Master time_elapsed      : ${MASTER_TIME}s"
        echo "    Max slave time_elapsed   : ${MAX_SLAVE_TIME}s"
        echo "    All slave times:"
        echo -e "$SLAVE_SUMMARY"
    } >> "$RESULTS_FILE"

    # Master log cleanup
    rm -f "$MASTER_LOG"

    # Cleanup cluster
    cleanup "$T" "${SLAVES[@]}"
    sleep 2
}

# ============================================================
#  MAIN
# ============================================================

# One-time SSH key setup
setup_ssh_keys

# Initialise results file
{
    echo "============================================================"
    echo " WMA/MSE Cluster Benchmark Results"
    echo " Date      : $(date)"
    echo " Matrix N  : $N"
    echo " Iterations: $ITERATIONS per slave count"
    echo "============================================================"
    echo ""
} > "$RESULTS_FILE"

# Loop over each slave count
for T in "${SLAVE_COUNTS[@]}"; do

    echo ""
    echo "=============================================="
    echo " Benchmarking t = $T slaves"
    echo "=============================================="

    # Slice the first T entries from ALL_SLAVES
    ACTIVE_SLAVES=("${ALL_SLAVES[@]:0:$T}")

    {
        echo "------------------------------------------------------------"
        echo "Slave count t = $T"
        echo "------------------------------------------------------------"
    } >> "$RESULTS_FILE"

    # Run ITERATIONS times
    for (( i=1; i<=ITERATIONS; i++ )); do
        echo "------------------------------------------"
        echo " t=$T | Iteration $i of $ITERATIONS"
        echo "------------------------------------------"

        run_iteration "$T" "$i" "${ACTIVE_SLAVES[@]}"

        echo "  Iteration $i complete."
        sleep 3
    done

    echo "" >> "$RESULTS_FILE"
done

# ---- Final summary ----
echo ""
echo "============================================================"
echo " All benchmarks complete!"
echo " Full results saved to: $RESULTS_FILE"
echo "============================================================"
cat "$RESULTS_FILE"