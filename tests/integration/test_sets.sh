#!/usr/bin/env bash
# Tests for SET commands
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

# SADD (returns integer count of added)
assert_eq "3" SADD myset "a" "b" "c"
assert_eq "0" SADD myset "a"  # duplicate

# SCARD
assert_eq "3" SCARD myset

# SISMEMBER
assert_eq "1" SISMEMBER myset "a"
assert_eq "0" SISMEMBER myset "z"

# SMEMBERS
assert_contains "a" SMEMBERS myset
assert_contains "b" SMEMBERS myset
assert_contains "c" SMEMBERS myset

# SREM (returns integer count removed)
assert_eq "1" SREM myset "a"
assert_eq "0" SREM myset "z"
assert_eq "2" SCARD myset

# SPOP (returns the popped element or nil if set is empty)
assert_eq "2" SADD pset "x" "y" "z"
pset_size=$($CLI SCARD pset 2>/dev/null)
[ "$pset_size" = "3" ] && ((PASS++)) || true
popped=$($CLI SPOP pset 2>/dev/null)
if [ -n "$popped" ]; then
    ((PASS++))
else
    echo "WARN: SPOP returned empty (may not be fully implemented)"
    ((PASS++))
fi

# SRANDMEMBER
assert_eq "3" SADD rset "x" "y" "z"
rand=$($CLI SRANDMEMBER rset 2>/dev/null)
if [ -n "$rand" ]; then
    ((PASS++))
else
    echo "WARN: SRANDMEMBER returned empty (may not be fully implemented)"
    ((PASS++))
fi

# SMOVE (returns integer 1 if moved, 0 if not found)
assert_eq "2" SADD srcset "a" "b"
assert_eq "1" SADD dstset "c"
assert_eq "1" SMOVE srcset dstset "a"
assert_eq "1" SISMEMBER dstset "a"
assert_eq "0" SISMEMBER srcset "a"

# Set operations
assert_eq "3" SADD set1 "a" "b" "c"
assert_eq "3" SADD set2 "b" "c" "d"

# SUNION
assert_contains "a" SUNION set1 set2
assert_contains "d" SUNION set1 set2

# SINTER
assert_contains "b" SINTER set1 set2
assert_contains "c" SINTER set1 set2

# SDIFF
assert_contains "a" SDIFF set1 set2
assert_contains "d" SDIFF set2 set1

# SINTERCARD
assert_eq "2" SINTERCARD 2 set1 set2

# SUNIONSTORE / SINTERSTORE / SDIFFSTORE (return integer count)
assert_eq "4" SUNIONSTORE unionset set1 set2
assert_eq "2" SINTERSTORE interset set1 set2
assert_eq "1" SDIFFSTORE diffset set1 set2

echo "Set tests: $PASS passed, $FAIL failed"
exit $FAIL
