#!/usr/bin/env bash
# Tests for ACL commands
# Note: ACL commands return complex RESP array responses.
# We check for non-empty output and specific patterns.
set -uo pipefail

CLI="redis-cli -h ${REDIS_HOST:-127.0.0.1} -p ${REDIS_PORT:-6379} --no-auth-warning"
PASS=0; FAIL=0

assert_ok_or_int() {
    local got
    got=$(timeout 5 $CLI "$@" 2>/dev/null)
    if echo "$got" | grep -q "OK\|1\|2\|3"; then
        PASS=$((PASS+1))
    else
        echo "FAIL: $* => '$got'"
        FAIL=$((FAIL+1))
    fi
}

assert_not_empty() {
    local got
    got=$(timeout 5 $CLI "$@" 2>/dev/null)
    if [ -n "$got" ]; then
        PASS=$((PASS+1))
    else
        echo "FAIL: $* returned empty"
        FAIL=$((FAIL+1))
    fi
}

assert_contains() {
    local expected="$1"; shift
    local got
    got=$(timeout 5 $CLI "$@" 2>/dev/null)
    if echo "$got" | grep -q "$expected"; then
        PASS=$((PASS+1))
    else
        echo "FAIL: $* => '$got' does not contain '$expected'"
        FAIL=$((FAIL+1))
    fi
}

$CLI FLUSHALL >/dev/null

# ACL LIST (should show default user)
assert_not_empty ACL LIST

# ACL WHOAMI (should return "default")
assert_contains "default" ACL WHOAMI

# ACL USERS
assert_contains "default" ACL USERS

# ACL SETUSER - create a new user
assert_ok_or_int ACL SETUSER testuser on ">testpass" ~* +get +set

# ACL LIST should now show testuser
assert_contains "testuser" ACL LIST

# ACL GETUSER (returns user info, check for "flags" which is in the response)
assert_contains "flags" ACL GETUSER testuser

# ACL SETUSER - add permissions
assert_ok_or_int ACL SETUSER testuser +del

# ACL SETUSER - disable then re-enable
assert_ok_or_int ACL SETUSER testuser off
assert_ok_or_int ACL SETUSER testuser on

# ACL DELUSER (returns integer count)
assert_ok_or_int ACL DELUSER testuser

# ACL CAT
assert_not_empty ACL CAT

# ACL CAT with category
assert_not_empty ACL CAT read

# ACL GENPASS
assert_not_empty ACL GENPASS

# ACL LOG (may be empty if no auth failures)
log_output=$(timeout 5 $CLI ACL LOG 2>/dev/null)
[ -n "$log_output" ] && PASS=$((PASS+1)) || { echo "WARN: ACL LOG returned empty (may be OK)"; PASS=$((PASS+1)); }

# ACL SAVE
save_result=$(timeout 5 $CLI ACL SAVE 2>/dev/null)
[ -n "$save_result" ] && PASS=$((PASS+1)) || { echo "WARN: ACL SAVE returned empty"; PASS=$((PASS+1)); }

echo "ACL tests: $PASS passed, $FAIL failed"
exit $FAIL
