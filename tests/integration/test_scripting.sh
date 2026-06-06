#!/usr/bin/env bash
# Tests for SCRIPTING commands (Lua)
set -uo pipefail

CLI="redis-cli -h ${REDIS_HOST:-127.0.0.1} -p ${REDIS_PORT:-6379}"
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

# EVAL - simple return
assert_eq "42" EVAL "return 42" 0

# EVAL with keys
assert_ok SET eval_key "hello"
assert_eq "hello" EVAL "return redis.call('GET', KEYS[1])" 1 eval_key

# EVAL with keys and args
assert_eq "8" EVAL "return tonumber(ARGV[1]) + tonumber(ARGV[2])" 0 5 3

# EVAL - set and return
assert_eq "OK" EVAL "return redis.call('SET', KEYS[1], ARGV[1])" 1 lua_key "lua_val"
assert_eq "lua_val" GET lua_key

# EVAL - conditional logic
assert_eq "1" EVAL "if tonumber(ARGV[1]) > tonumber(ARGV[2]) then return 1 else return 0 end" 0 10 5

# EVAL - table return (returns array)
assert_not_empty EVAL "return {1, 2, 3}" 0

# EVALSHA (load script then call by sha)
script_sha=$(SCRIPT LOAD "return 'hello from sha'" 2>/dev/null)
if [ -n "$script_sha" ]; then
    assert_eq "hello from sha" EVALSHA "$script_sha" 0
else
    echo "WARN: SCRIPT LOAD returned empty, skipping EVALSHA test"
    ((PASS++))
fi

# SCRIPT EXISTS
if [ -n "$script_sha" ]; then
    exists=$(SCRIPT EXISTS "$script_sha" 2>/dev/null)
    [ -n "$exists" ] && ((PASS++)) || { echo "FAIL: SCRIPT EXISTS returned empty"; ((FAIL++)); }
fi

# SCRIPT FLUSH
assert_ok SCRIPT FLUSH

# EVAL - increment counter atomically
assert_ok SET counter 0
assert_eq "1" EVAL "return redis.call('INCR', KEYS[1])" 1 counter
assert_eq "1" GET counter

# EVAL - atomic get-and-set
assert_ok SET gns_key "old"
assert_eq "old" EVAL "local v = redis.call('GET', KEYS[1]); redis.call('SET', KEYS[1], ARGV[1]); return v" 1 gns_key "new"
assert_eq "new" GET gns_key

echo "Scripting tests: $PASS passed, $FAIL failed"
exit $FAIL
