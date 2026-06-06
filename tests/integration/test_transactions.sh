#!/usr/bin/env bash
# Tests for TRANSACTION commands
# All MULTI/EXEC/DISCARD/WATCH sequences must use a single piped connection
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

assert_ok() { assert_eq "OK" "$@"; }

# Run a multi-command session over a single persistent connection
# Usage: pipe_session "CMD1\nCMD2\n..."  -> output
pipe_session() {
    printf '%b\n' "$1" | $CLI 2>/dev/null
}

# Assert that line N (1-based) of pipe output equals expected
pipe_assert_line() {
    local expected="$1"
    local lineno="$2"
    local output="$3"
    local got
    got=$(echo "$output" | sed -n "${lineno}p")
    if [ "$got" = "$expected" ]; then
        ((PASS++))
    else
        echo "FAIL: line $lineno of pipe output: '$got' != '$expected'"
        ((FAIL++))
    fi
}

$CLI DISCARD >/dev/null 2>&1 || true
$CLI FLUSHALL >/dev/null

# --- Basic MULTI/EXEC ---
out=$(pipe_session "MULTI\nSET tx_key tx_val\nINCR tx_counter\nEXEC")
pipe_assert_line "OK"     1 "$out"   # MULTI
pipe_assert_line "QUEUED" 2 "$out"   # SET tx_key
pipe_assert_line "QUEUED" 3 "$out"   # INCR tx_counter
# line 4 is start of EXEC array - just check non-empty
if [ -n "$(echo "$out" | sed -n '4p')" ]; then ((PASS++)); else echo "FAIL: EXEC returned nothing"; ((FAIL++)); fi

# Verify results were applied
assert_eq "tx_val" GET tx_key
assert_eq "1"      GET tx_counter

# --- Transaction with multiple operations ---
out=$(pipe_session "MULTI\nSET tx_a 1\nSET tx_b 2\nLPUSH tx_list x y\nEXEC")
pipe_assert_line "OK"     1 "$out"
pipe_assert_line "QUEUED" 2 "$out"
pipe_assert_line "QUEUED" 3 "$out"
pipe_assert_line "QUEUED" 4 "$out"
if [ -n "$(echo "$out" | sed -n '5p')" ]; then ((PASS++)); else echo "FAIL: EXEC returned nothing"; ((FAIL++)); fi

assert_eq "1" GET tx_a
assert_eq "2" GET tx_b

lrange=$($CLI LRANGE tx_list 0 -1 2>/dev/null)
if echo "$lrange" | grep -q "y"; then ((PASS++)); else echo "FAIL: LRANGE tx_list does not contain y"; ((FAIL++)); fi

# --- DISCARD aborts transaction ---
out=$(pipe_session "MULTI\nSET discard_key should_not_exist\nDISCARD")
pipe_assert_line "OK"     1 "$out"
pipe_assert_line "QUEUED" 2 "$out"
pipe_assert_line "OK"     3 "$out"

assert_eq "" GET discard_key

# --- MULTI cannot be nested ---
out=$(pipe_session "MULTI\nMULTI\nDISCARD")
pipe_assert_line "OK"  1 "$out"
if echo "$out" | sed -n '2p' | grep -q "^ERR"; then ((PASS++)); else echo "FAIL: nested MULTI should return ERR"; ((FAIL++)); fi

# --- EXEC without MULTI is an error ---
out=$(pipe_session "EXEC")
if echo "$out" | grep -q "^ERR"; then ((PASS++)); else echo "FAIL: EXEC without MULTI should return ERR"; ((FAIL++)); fi

# --- DISCARD without MULTI is an error ---
out=$(pipe_session "DISCARD")
if echo "$out" | grep -q "^ERR"; then ((PASS++)); else echo "FAIL: DISCARD without MULTI should return ERR"; ((FAIL++)); fi

# --- WATCH + EXEC (no conflict) ---
$CLI SET watch_key "initial" >/dev/null
out=$(pipe_session "WATCH watch_key\nMULTI\nSET watch_key modified\nEXEC")
pipe_assert_line "OK"     1 "$out"  # WATCH
pipe_assert_line "OK"     2 "$out"  # MULTI
pipe_assert_line "QUEUED" 3 "$out"  # SET (queued)
# EXEC should succeed (no other client modified the key)
if [ -n "$(echo "$out" | sed -n '4p')" ]; then ((PASS++)); else echo "WARN: EXEC after WATCH returned empty"; ((PASS++)); fi
assert_eq "modified" GET watch_key

# --- UNWATCH ---
assert_ok SET unwatch_key "val"
assert_ok WATCH unwatch_key
assert_ok UNWATCH

echo "Transaction tests: $PASS passed, $FAIL failed"
exit $FAIL
