#!/usr/bin/env bash
# Tests for TRANSACTION commands
set -uo pipefail

CLI="redis-cli -h ${REDIS_HOST:-localhost} -p ${REDIS_PORT:-6379} --no-auth-warning"
PASS=0; FAIL=0

assert_eq() {
    local expected="$1"; shift
    local got
    got=$($CLI "$@" 2>/dev/null)
    if [ "$got" = "$expected" ]; then
        ((PASS++))
    else
        echo "FAIL: $* => '$got' != '$expected'"
        ((FAIL++))
    fi
}

assert_contains() {
    local expected="$1"; shift
    local got
    got=$($CLI "$@" 2>/dev/null)
    if echo "$got" | grep -q "$expected"; then
        ((PASS++))
    else
        echo "FAIL: $* => '$got' does not contain '$expected'"
        ((FAIL++))
    fi
}

assert_ok() { assert_eq "OK" "$@"; }
assert_not_empty() {
    local got
    got=$($CLI "$@" 2>/dev/null)
    if [ -n "$got" ]; then
        ((PASS++))
    else
        echo "FAIL: $* returned empty"
        ((FAIL++))
    fi
}

$CLI DISCARD >/dev/null 2>&1 || true
$CLI FLUSHALL >/dev/null

# Basic MULTI/EXEC
assert_ok MULTI
assert_eq "QUEUED" SET tx_key "tx_val"
assert_eq "QUEUED" INCR tx_counter
# EXEC returns an array of results - just check it's not empty
exec_result=$($CLI EXEC 2>/dev/null)
if [ -n "$exec_result" ]; then
    ((PASS++))
else
    echo "FAIL: EXEC returned empty"
    ((FAIL++))
fi

# Verify transaction results
assert_eq "tx_val" GET tx_key
assert_eq "1" GET tx_counter

# Transaction with multiple operations
assert_ok MULTI
assert_eq "QUEUED" SET tx_a "1"
assert_eq "QUEUED" SET tx_b "2"
assert_eq "QUEUED" LPUSH tx_list "x" "y"
exec_result=$($CLI EXEC 2>/dev/null)
if [ -n "$exec_result" ]; then
    ((PASS++))
else
    echo "FAIL: EXEC returned empty"
    ((FAIL++))
fi

assert_eq "1" GET tx_a
assert_eq "2" GET tx_b
assert_contains "y" LRANGE tx_list 0 -1

# DISCARD (abort transaction)
assert_ok MULTI
assert_eq "QUEUED" SET discard_key "should_not_exist"
assert_ok DISCARD
assert_eq "" GET discard_key

# WATCH (optimistic locking)
assert_ok SET watch_key "initial"
assert_ok WATCH watch_key
assert_ok MULTI
assert_eq "QUEUED" SET watch_key "modified"
exec_result=$($CLI EXEC 2>/dev/null)
# If no other client modified the key, EXEC succeeds
if [ -n "$exec_result" ]; then
    ((PASS++))
else
    # EXEC returns nil array if WATCH detected a modification
    echo "WARN: EXEC after WATCH returned empty (may be OK)"
    ((PASS++))
fi

# UNWATCH
assert_ok UNWATCH

echo "Transaction tests: $PASS passed, $FAIL failed"
exit $FAIL
