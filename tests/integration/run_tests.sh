#!/usr/bin/env bash
# Integration test runner for valkey-rs
# Spins up valkey-rs via Docker Compose, runs all test scripts, tears down.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
REDIS_PORT="${REDIS_PORT:-6379}"

COMPOSE="docker compose -f $PROJECT_ROOT/docker-compose.yml"

PASS_TOTAL=0
FAIL_TOTAL=0
SUITES_PASSED=0
SUITES_FAILED=0

echo "============================================"
echo " valkey-rs Integration Tests"
echo "============================================"
echo

# --- Start container ---
echo "[1/4] Starting valkey-rs via Docker Compose..."
cd "$PROJECT_ROOT"
$COMPOSE down -v --remove-orphans 2>/dev/null || true
$COMPOSE up -d --build

# --- Wait for healthy ---
echo "[2/4] Waiting for valkey-rs to accept connections..."
READY=0
for i in $(seq 1 60); do
    if python3 "$SCRIPT_DIR/check_connectivity.py" "$REDIS_HOST" "$REDIS_PORT" 2>/dev/null; then
        echo "  valkey-rs is ready (attempt $i)"
        READY=1
        break
    fi
    sleep 1
done

if [ "$READY" -ne 1 ]; then
    echo "ERROR: valkey-rs did not become ready within 60s"
    $COMPOSE logs valkey 2>/dev/null || true
    $COMPOSE down -v
    exit 1
fi

# --- Run test suites ---
echo
echo "[3/4] Running test suites..."

for test_script in "$SCRIPT_DIR"/test_*.sh; do
    suite_file="$(basename "$test_script")"
    suite_name="${suite_file%.sh}"
    echo
    echo "--- $suite_name ---"

    # Check connectivity before each test suite
    if ! python3 "$SCRIPT_DIR/check_connectivity.py" "$REDIS_HOST" "$REDIS_PORT" 2>/dev/null; then
        echo "ERROR: Server not responding, attempting restart..."
        $COMPOSE restart valkey 2>/dev/null
        sleep 3
        for j in $(seq 1 30); do
            if python3 "$SCRIPT_DIR/check_connectivity.py" "$REDIS_HOST" "$REDIS_PORT" 2>/dev/null; then
                echo "  valkey-rs is back (attempt $j)"
                break
            fi
            sleep 1
        done
    fi

    # Run tests from the host side connecting to the container's forwarded port
    output=$(REDIS_HOST="$REDIS_HOST" REDIS_PORT="$REDIS_PORT" bash "$test_script" 2>&1) || true

    echo "$output"

    # Extract pass/fail counts from the script output
    suite_pass=$(echo "$output" | grep -oP '\d+(?= passed)' | tail -1)
    suite_fail=$(echo "$output" | grep -oP '\d+(?= failed)' | tail -1)

    suite_pass="${suite_pass:-0}"
    suite_fail="${suite_fail:-0}"

    PASS_TOTAL=$((PASS_TOTAL + suite_pass))
    FAIL_TOTAL=$((FAIL_TOTAL + suite_fail))

    if [ "$suite_fail" -gt 0 ]; then
        SUITES_FAILED=$((SUITES_FAILED + 1))
    elif [ "$suite_pass" -gt 0 ]; then
        SUITES_PASSED=$((SUITES_PASSED + 1))
    fi
done

# --- Cleanup ---
echo
echo "[4/4] Tearing down..."
$COMPOSE down -v --remove-orphans

# --- Summary ---
echo
echo "============================================"
echo " Integration Test Results"
echo "============================================"
echo " Suites passed: $SUITES_PASSED / $((SUITES_PASSED + SUITES_FAILED))"
echo " Assertions passed: $PASS_TOTAL"
echo " Assertions failed: $FAIL_TOTAL"
echo "============================================"

if [ "$FAIL_TOTAL" -gt 0 ]; then
    echo "OVERALL: FAIL"
    exit 1
else
    echo "OVERALL: PASS"
    exit 0
fi
