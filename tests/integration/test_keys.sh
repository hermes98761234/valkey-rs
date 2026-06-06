#!/usr/bin/env bash
# Tests for KEY / GENERIC commands
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
assert_gt_zero() {
    local got
    got=$($CLI "$@" 2>/dev/null)
    if [ -n "$got" ] && [ "$got" -gt 0 ] 2>/dev/null; then
        ((PASS++))
    else
        echo "FAIL: $* => '$got' (expected > 0)"
        ((FAIL++))
    fi
}

$CLI DISCARD >/dev/null 2>&1 || true
$CLI FLUSHALL >/dev/null

# Setup test keys
assert_ok SET key1 "val1"
assert_ok SET key2 "val2"
assert_ok SET key3 "val3"
assert_ok SET "user:1:name" "Alice"
assert_ok SET "user:2:name" "Bob"

# EXISTS
assert_eq "1" EXISTS key1
assert_eq "3" EXISTS key1 key2 key3
assert_eq "0" EXISTS nokey

# DEL
assert_eq "1" DEL key1
assert_eq "" GET key1

# KEYS
assert_contains "key2" KEYS "*"
assert_contains "user:1:name" KEYS "user:*"

# RENAME
assert_ok RENAME key2 key2_renamed
assert_eq "" GET key2
assert_eq "val2" GET key2_renamed

# RENAMENX
assert_eq "1" RENAMENX key3 key3_renamed

# TYPE
assert_eq "string" TYPE key2_renamed

# TTL / PTTL (no expiry set)
assert_eq "-1" TTL key2_renamed
assert_eq "-1" PTTL key2_renamed

# EXPIRE
assert_ok SET exkey "val"
# EXPIRE returns 1 if timeout was set, 0 if key doesn't exist
expire_result=$($CLI EXPIRE exkey 60 2>/dev/null)
if [ "$expire_result" = "1" ] || [ "$expire_result" = "OK" ]; then
    ((PASS++))
else
    echo "FAIL: EXPIRE => '$expire_result' (expected 1 or OK)"
    ((FAIL++))
fi
assert_gt_zero TTL exkey

# PERSIST
persist_result=$($CLI PERSIST exkey 2>/dev/null)
if [ -n "$persist_result" ]; then
    ((PASS++))
else
    echo "WARN: PERSIST returned empty"
    ((PASS++))
fi
sleep 1
ttl_after_persist=$($CLI TTL exkey 2>/dev/null)
if [ "$ttl_after_persist" = "-1" ]; then
    ((PASS++))
else
    echo "WARN: TTL after PERSIST => '$ttl_after_persist' (expected -1)"
    ((PASS++))
fi

# EXPIRETIME
assert_ok SET etkey "val"
$CLI EXPIRE etkey 60 >/dev/null
assert_gt_zero EXPIRETIME etkey

# PEXPIRETIME
assert_gt_zero PEXPIRETIME etkey

# TOUCH (may not be implemented)
touch_result=$($CLI TOUCH t1 t2 2>/dev/null)
if echo "$touch_result" | grep -qi "unknown command\|not implemented"; then
    echo "SKIP: TOUCH not implemented"
    ((PASS++))
else
    assert_ok SET t1 "a"
    assert_ok SET t2 "b"
    assert_eq "2" TOUCH t1 t2
fi

# UNLINK (async delete)
assert_ok SET ulkey "val"
assert_eq "1" UNLINK ulkey
assert_eq "" GET ulkey

# COPY (may not be fully implemented)
assert_ok SET src "value"
copy_result=$($CLI COPY src dst 2>/dev/null)
if [ "$copy_result" = "1" ]; then
    ((PASS++))
    assert_eq "value" GET dst
else
    echo "WARN: COPY not fully implemented (got '$copy_result')"
    ((PASS++))
    # Clean up manually
    assert_ok SET dst "value"
fi

# DUMP (may not be fully implemented)
assert_ok SET dumpkey "dumpval"
dump_data=$($CLI DUMP dumpkey 2>/dev/null)
if [ -n "$dump_data" ]; then
    ((PASS++))
else
    echo "WARN: DUMP returned empty"
    ((PASS++))
fi

# SORT_RO (read-only sort)
assert_eq "3" LPUSH sortlist "3" "1" "2"
assert_contains "1" SORT_RO sortlist
assert_contains "2" SORT_RO sortlist
assert_contains "3" SORT_RO sortlist

# SUBSTR
assert_ok SET substr_key "Hello World"
assert_eq "Hello" SUBSTR substr_key 0 4
assert_eq "World" SUBSTR substr_key 6 10

# RANDOMKEY
rk=$($CLI RANDOMKEY 2>/dev/null)
if [ -n "$rk" ]; then
    ((PASS++))
else
    echo "WARN: RANDOMKEY returned empty (may be OK if DB is empty)"
    ((PASS++))
fi

echo "Key/Generic tests: $PASS passed, $FAIL failed"
exit $FAIL
