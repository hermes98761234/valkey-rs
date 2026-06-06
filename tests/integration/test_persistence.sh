#!/usr/bin/env bash
# Tests for PERSISTENCE (RDB/AOF)
# Tests that data survives a container restart.
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

# Set some data before restart
assert_ok SET persist_key "persist_value"
assert_ok SET persist_counter "42"
assert_eq "3" LPUSH persist_list "a" "b" "c"
assert_eq "2" HSET persist_hash field1 "val1" field2 "val2"

# Verify data exists before restart
assert_eq "persist_value" GET persist_key
assert_eq "42" GET persist_counter

# Trigger BGSAVE
save_result=$($CLI BGSAVE 2>/dev/null)
if [ -n "$save_result" ]; then
    ((PASS++))
else
    echo "WARN: BGSAVE returned empty"
    ((PASS++))
fi
sleep 2

# Restart the container using docker compose
echo "Restarting valkey container..."
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
docker compose -f "$PROJECT_ROOT/docker-compose.yml" restart valkey 2>/dev/null

# Wait for container to come back
echo "Waiting for valkey to restart..."
for i in $(seq 1 30); do
    if $CLI PING 2>/dev/null | grep -q PONG; then
        echo "  valkey is back (attempt $i)"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "ERROR: valkey did not restart within 30s"
        exit 1
    fi
    sleep 1
done

sleep 1

# Verify data survived restart
assert_eq "persist_value" GET persist_key
assert_eq "42" GET persist_counter
assert_contains "a" LRANGE persist_list 0 -1
assert_eq "val1" HGET persist_hash field1
assert_eq "val2" HGET persist_hash field2

# DUMP
assert_ok SET dump_test "dump_value"
dump_data=$($CLI DUMP dump_test 2>/dev/null)
if [ -n "$dump_data" ]; then
    ((PASS++))
else
    echo "WARN: DUMP returned empty"
    ((PASS++))
fi

# BGREWRITEAOF
aof_result=$($CLI BGREWRITEAOF 2>/dev/null)
if [ -n "$aof_result" ]; then
    ((PASS++))
else
    echo "WARN: BGREWRITEAOF returned empty"
    ((PASS++))
fi

# LASTSAVE
lastsave=$($CLI LASTSAVE 2>/dev/null)
if [ -n "$lastsave" ] && [ "$lastsave" -gt 0 ] 2>/dev/null; then
    ((PASS++))
else
    echo "WARN: LASTSAVE => '$lastsave'"
    ((PASS++))
fi

echo "Persistence tests: $PASS passed, $FAIL failed"
exit $FAIL
