#!/bin/bash
# ============================================================
#  Local Orchestrator — WMA/MSE Benchmark
#  Runs t = 2, 4, 8, 16 slaves × 3 iterations each
# ============================================================

# ===== USER CONFIG =====
PROGRAM_PATH="./main"  # Updated for local execution
N=4000
MASTER_IP="127.0.0.1"

# All 16 local slaves with 300x ports
ALL_SLAVES=(
    "127.0.0.1 3001"
    "127.0.0.1 3002"
    "127.0.0.1 3003"
    "127.0.0.1 3004"
    "127.0.0.1 3005"
    "127.0.0.1 3006"
    "127.0.0.1 3007"
    "127.0.0.1 3008"
    "127.0.0.1 3009"
    "127.0.0.1 3010"
    "127.0.0.1 3011"
    "127.0.0.1 3012"
    "127.0.0.1 3013"
    "127.0.0.1 3014"
    "127.0.0.1 3015"
    "127.0.0.1 3016"
)

RESULTS_FILE="benchmark_results.txt"
SLAVE_COUNTS=(2 4 8 16)
ITERATIONS=3
# =======================

cleanup() {
    # Kill local processes instead of SSH killall
    pkill -f "$PROGRAM_PATH" 2>/dev/null
    wait
}

run_iteration() {
    local T=$1
    local ITER=$2
    shift 2
    local SLAVES=("$@")

    echo ""
    echo "  [t=$T | Iter $ITER] Launching $T local slaves..."

    declare -a SLAVE_LOG_FILES

    for entry in "${SLAVES[@]}"; do
        read -r IP PORT <<< "$entry"
        SLAVE_LOG=$(mktemp /tmp/slave_${PORT}_XXXXXX.log)
        SLAVE_LOG_FILES+=("$SLAVE_LOG $IP $PORT")

        # Run locally
        echo -e "$N\n$PORT\n1" | $PROGRAM_PATH > "$SLAVE_LOG" 2>&1 &
    done

    echo "  [t=$T | Iter $ITER] Waiting 5s for local initialization..."
    sleep 5

    MASTER_LOG=$(mktemp /tmp/master_XXXXXX.log)
    echo "  [t=$T | Iter $ITER] Launching master locally..."
    echo -e "$N\n3000\n0" | $PROGRAM_PATH > "$MASTER_LOG" 2>&1

    # Parse results
    MASTER_TIME=$(grep -oP 'time_elapsed\s*=\s*\K[0-9]+\.[0-9]+' "$MASTER_LOG" | head -1)
    MASTER_TIME=${MASTER_TIME:-"N/A"}

    MAX_SLAVE_TIME="0"
    SLAVE_SUMMARY=""

    for entry in "${SLAVE_LOG_FILES[@]}"; do
        read -r LOG_FILE IP PORT <<< "$entry"
        SLAVE_TIME=$(grep -oP 'time_elapsed.*?=\s*\K[0-9]+\.[0-9]+' "$LOG_FILE" | head -1)
        SLAVE_TIME=${SLAVE_TIME:-"N/A"}
        SLAVE_SUMMARY+="      Slave $IP:$PORT => ${SLAVE_TIME}s\n"

        if [[ "$SLAVE_TIME" != "N/A" ]]; then
            if (( $(echo "$SLAVE_TIME > $MAX_SLAVE_TIME" | bc -l) )); then
                MAX_SLAVE_TIME=$SLAVE_TIME
            fi
        fi
        rm -f "$LOG_FILE"
    done

    [[ "$MAX_SLAVE_TIME" == "0" ]] && MAX_SLAVE_TIME="N/A"

    echo "  [t=$T | Iter $ITER] Master time     : ${MASTER_TIME}s"
    echo "  [t=$T | Iter $ITER] Max slave time  : ${MAX_SLAVE_TIME}s"

    {
        echo "  Iteration $ITER:"
        echo "    Master time_elapsed      : ${MASTER_TIME}s"
        echo "    Max slave time_elapsed   : ${MAX_SLAVE_TIME}s"
        echo "    All slave times:"
        echo -e "$SLAVE_SUMMARY"
    } >> "$RESULTS_FILE"

    rm -f "$MASTER_LOG"
    cleanup
    sleep 2
}

# --- MAIN ---
cleanup
{
    echo "============================================================"
    echo " Local WMA/MSE Benchmark Results"
    echo " Date      : $(date)"
    echo " Matrix N  : $N"
    echo "============================================================"
} > "$RESULTS_FILE"

for T in "${SLAVE_COUNTS[@]}"; do
    echo "=============================================="
    echo " Benchmarking t = $T local slaves"
    echo "=============================================="
    ACTIVE_SLAVES=("${ALL_SLAVES[@]:0:$T}")

    echo "------------------------------------------------------------" >> "$RESULTS_FILE"
    echo "Slave count t = $T" >> "$RESULTS_FILE"

    for (( i=1; i<=ITERATIONS; i++ )); do
        run_iteration "$T" "$i" "${ACTIVE_SLAVES[@]}"
    done
done

cat "$RESULTS_FILE"