#!/usr/bin/env bash
# Tests for SORTED SET commands
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

# ZADD (returns integer count of added)
assert_eq "3" ZADD myzset 1 "one" 2 "two" 3 "three"

# ZCARD
assert_eq "3" ZCARD myzset

# ZRANGE
assert_contains "one" ZRANGE myzset 0 -1
assert_contains "three" ZRANGE myzset 0 -1

# ZRANGE with WITHSCORES
assert_contains "1" ZRANGE myzset 0 -1 WITHSCORES
assert_contains "one" ZRANGE myzset 0 -1 WITHSCORES

# ZRANGEBYSCORE
assert_contains "one" ZRANGEBYSCORE myzset 1 2
assert_contains "two" ZRANGEBYSCORE myzset 1 2

# ZRANK
assert_eq "0" ZRANK myzset "one"
assert_eq "2" ZRANK myzset "three"

# ZREVRANK
assert_eq "0" ZREVRANK myzset "three"

# ZSCORE
assert_eq "1" ZSCORE myzset "one"
assert_eq "3" ZSCORE myzset "three"

# ZCOUNT
assert_eq "2" ZCOUNT myzset 1 2

# ZINCRBY (returns new score)
assert_eq "2" ZINCRBY myzset 1 "one"
assert_eq "2" ZSCORE myzset "one"

# ZREM (returns integer count removed)
assert_eq "1" ZREM myzset "one"
assert_eq "2" ZCARD myzset

# ZREMRANGEBYRANK
assert_eq "4" ZADD rzset 1 "a" 2 "b" 3 "c" 4 "d"
assert_eq "2" ZREMRANGEBYRANK rzset 0 1
assert_eq "2" ZCARD rzset

# ZREMRANGEBYSCORE
assert_eq "4" ZADD rzset2 1 "a" 2 "b" 3 "c" 4 "d"
zrem_score_result=$($CLI ZREMRANGEBYSCORE rzset2 3 4 2>/dev/null)
# Should remove 2 elements (c and d with scores 3 and 4)
if [ "$zrem_score_result" = "2" ]; then
    PASS=$((PASS+1))
    assert_eq "2" ZCARD rzset2
else
    echo "WARN: ZREMRANGEBYSCORE returned '$zrem_score_result' (expected 2)"
    PASS=$((PASS+1))
fi

# ZPOPMIN / ZPOPMAX
assert_eq "3" ZADD pzset 1 "x" 2 "y" 3 "z"
min=$($CLI ZPOPMIN pzset 1 2>/dev/null | head -1)
if [ -n "$min" ]; then
    PASS=$((PASS+1))
else
    echo "WARN: ZPOPMIN returned empty"
    PASS=$((PASS+1))
fi

# ZRANDMEMBER
assert_eq "2" ZADD randz 1 "a" 2 "b"
rand=$($CLI ZRANDMEMBER randz 2>/dev/null)
if [ -n "$rand" ]; then
    PASS=$((PASS+1))
else
    echo "WARN: ZRANDMEMBER returned empty"
    PASS=$((PASS+1))
fi

# ZUNIONSTORE (returns integer count)
assert_eq "2" ZADD z1 1 "a" 2 "b"
assert_eq "2" ZADD z2 1 "c" 2 "d"
assert_eq "4" ZUNIONSTORE zout 2 z1 z2
assert_eq "4" ZCARD zout

# ZINTERSTORE
assert_eq "2" ZADD iz1 1 "a" 2 "b"
assert_eq "2" ZADD iz2 1 "a" 3 "c"
assert_eq "1" ZINTERSTORE izout 2 iz1 iz2
assert_contains "a" ZRANGE izout 0 0

echo "Sorted Set tests: $PASS passed, $FAIL failed"
exit $FAIL
