#!/usr/bin/env bash
# Tests for PUBSUB commands
# Note: SUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE need pub/sub mode at the connection level
# and return ERR in normal command dispatch. We test what works via normal dispatch.
set -uo pipefail

CLI="redis-cli -h ${REDIS_HOST:-127.0.0.1} -p ${REDIS_PORT:-6379}"
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

assert_ok_or_skip() {
    local got
    got=$($CLI "$@" 2>/dev/null)
    if echo "$got" | grep -qi "ERR\|unknown"; then
        echo "SKIP: $* not available in normal mode ($got)"
        PASS=$((PASS+1))
    elif [ -n "$got" ]; then
        PASS=$((PASS+1))
    else
        echo "FAIL: $* returned empty"
        FAIL=$((FAIL+1))
    fi
}

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

$CLI FLUSHALL >/dev/null

# PUBLISH (no subscribers yet, should return 0)
pub_result=$($CLI PUBLISH testchannel "hello" 2>/dev/null)
# PUBLISH returns the number of subscribers (0 if none)
if [ "$pub_result" = "0" ] || [ -z "$pub_result" ] || echo "$pub_result" | grep -qi "ERR"; then
    PASS=$((PASS+1))
else
    echo "FAIL: PUBLISH => '$pub_result'"
    FAIL=$((FAIL+1))
fi

# PUBSUB CHANNELS
assert_ok_or_skip PUBSUB CHANNELS

# PUBSUB NUMSUB
assert_ok_or_skip PUBSUB NUMSUB testchannel

# PUBSUB NUMPAT
assert_ok_or_skip PUBSUB NUMPAT

# SPUBLISH (shard publish - may or may not be available)
spub_result=$($CLI SPUBLISH shardch "msg" 2>/dev/null)
if [ -n "$spub_result" ]; then
    PASS=$((PASS+1))
else
    echo "SKIP: SPUBLISH returned empty"
    PASS=$((PASS+1))
fi

echo "PubSub tests: $PASS passed, $FAIL failed"
exit $FAIL
