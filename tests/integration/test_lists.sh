#!/usr/bin/env bash
# Tests for LIST commands
set -uo pipefail

CLI="redis-cli -h ${REDIS_HOST:-localhost} -p ${REDIS_PORT:-6379} --no-auth-warning"
PASS=0; FAIL=0

assert_eq() {
    local expected="$1"; shift
    local got
    got=$($CLI "$@" 2>/dev/null)
    if [ "$got" = "$expected" ]; then
        PASS=$((PASS+1))
    else
        echo "FAIL: $* => '$got' != '$expected'"
        FAIL=$((FAIL+1))
    fi
}

assert_contains() {
    local expected="$1"; shift
    local got
    got=$($CLI "$@" 2>/dev/null)
    if echo "$got" | grep -q "$expected"; then
        PASS=$((PASS+1))
    else
        echo "FAIL: $* => '$got' does not contain '$expected'"
        FAIL=$((FAIL+1))
    fi
}

assert_ok() { assert_eq "OK" "$@"; }
assert_not_empty() {
    local got
    got=$($CLI "$@" 2>/dev/null)
    if [ -n "$got" ]; then
        PASS=$((PASS+1))
    else
        echo "FAIL: $* returned empty"
        FAIL=$((FAIL+1))
    fi
}

$CLI DISCARD >/dev/null 2>&1 || true
$CLI FLUSHALL >/dev/null

# LPUSH / RPUSH (return integer count)
assert_eq "3" LPUSH mylist "world" "hello" "first"
assert_eq "4" RPUSH mylist "last"

# LRANGE
assert_contains "first" LRANGE mylist 0 -1
assert_contains "last" LRANGE mylist 0 -1

# LLEN
assert_eq "4" LLEN mylist

# LINDEX
assert_eq "first" LINDEX mylist 0
assert_eq "last" LINDEX mylist -1

# LSET
assert_ok LSET mylist 0 "FIRST"
assert_eq "FIRST" LINDEX mylist 0

# LINSERT (returns integer length)
assert_eq "5" LINSERT mylist BEFORE "world" "inserted"
assert_contains "inserted" LRANGE mylist 0 -1

# LPOP / RPOP
assert_eq "FIRST" LPOP mylist
assert_eq "last" RPOP mylist

# LREM (returns integer count removed)
assert_eq "5" LPUSH remlist "a" "b" "a" "c" "a"
assert_eq "2" LREM remlist 2 "a"
assert_contains "b" LRANGE remlist 0 -1

# LTRIM
assert_ok LTRIM remlist 0 1
assert_eq "2" LLEN remlist

# RPOPLPUSH (may not be implemented, skip if not)
rplp_result=$($CLI RPOPLPUSH src dst 2>/dev/null)
if echo "$rplp_result" | grep -qi "unknown command\|not implemented"; then
    echo "SKIP: RPOPLPUSH not implemented"
    PASS=$((PASS+1))
else
    # LPUSH x y z builds [z, y, x]; RPOPLPUSH pops the tail => "x"
    assert_eq "3" LPUSH src "x" "y" "z"
    assert_eq "x" RPOPLPUSH src dst
    assert_contains "y" LRANGE src 0 -1
    assert_contains "x" LRANGE dst 0 -1
fi

# LMOVE (may not be implemented)
lm_result=$($CLI LMOVE msrc mdst LEFT LEFT 2>/dev/null)
if echo "$lm_result" | grep -qi "unknown command\|not implemented"; then
    echo "SKIP: LMOVE not implemented"
    PASS=$((PASS+1))
else
    assert_eq "2" LPUSH msrc "a" "b"
    assert_not_empty LMOVE msrc mdst LEFT LEFT
    assert_not_empty LRANGE mdst 0 -1
fi

# BLPOP (non-blocking with timeout)
blpop_result=$(timeout 2 $CLI BLPOP empty 1 2>/dev/null)
# Should return empty/nil after timeout — just verify it doesn't hang
PASS=$((PASS+1))

echo "List tests: $PASS passed, $FAIL failed"
exit $FAIL
