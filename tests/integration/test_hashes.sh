#!/usr/bin/env bash
# Tests for HASH commands
set -uo pipefail

CLI="redis-cli -h ${REDIS_HOST:-127.0.0.1} -p ${REDIS_PORT:-6379} --no-auth-warning"
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

$CLI FLUSHALL >/dev/null

# HSET / HGET
assert_eq "1" HSET user:1 name "Alice"
assert_eq "Alice" HGET user:1 name

# HSET multiple fields
assert_eq "2" HSET user:1 age "30" email "alice@example.com"
assert_eq "30" HGET user:1 age
assert_eq "alice@example.com" HGET user:1 email

# HGETALL
assert_contains "name" HGETALL user:1
assert_contains "Alice" HGETALL user:1

# HDEL
assert_eq "1" HDEL user:1 email
assert_eq "" HGET user:1 email

# HEXISTS
assert_eq "1" HEXISTS user:1 name
assert_eq "0" HEXISTS user:1 email

# HLEN
assert_eq "2" HLEN user:1

# HKEYS
assert_contains "name" HKEYS user:1
assert_contains "age" HKEYS user:1

# HVALS
assert_contains "Alice" HVALS user:1
assert_contains "30" HVALS user:1

# HINCRBY
assert_eq "31" HINCRBY user:1 age 1

# HINCRBYFLOAT
assert_eq "31.5" HINCRBYFLOAT user:1 age 0.5

# HMSET (legacy, should still work)
assert_ok HMSET user:2 name "Bob" age "25"
assert_eq "Bob" HGET user:2 name

# HRANDFIELD (returns a random field — just check not empty)
assert_not_empty HRANDFIELD user:1

echo "Hash tests: $PASS passed, $FAIL failed"
exit $FAIL
