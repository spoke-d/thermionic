#!/bin/sh -e
[ -n "${GOPATH:-}" ] && export "PATH=${GOPATH}/bin:${PATH}"

# Force UTC for consistency
export TZ="UTC"

if [ -n "${THERMIONIC_VERBOSE:-}" ] || [ -n "${THERMIONIC_DEBUG:-}" ]; then
  set -x
fi

export DEBUG=""
if [ -n "${THERMIONIC_VERBOSE}:-" ]; then
    DEBUG="--verbose"
fi

if [ -n "${THERMIONIC_DEBUG}:-" ]; then
    DEBUG="--debug"
fi

import_subdir_files() {
    test "$1"
    local file
    for file in "$1"/*.sh; do
        . "$file"
    done
}

import_subdir_files includes

echo "==> Checking for dependencies"
check_dependencies curl therm sqlite3

if [ "${USER:-'root'}" = "root" ]; then
    echo "The testsuite must not be run as root." >&2
    exit 1
fi

cleanup() {
    # Allow for failures and stop tracing everything
    set +ex
    DEBUG=

    # Allow for inspection
    if [ -n "${TEST_INSPECT:-}" ]; then
        if [ "${TEST_RESULT}" != "success" ]; then
            echo "==> TEST DONE: ${TEST_CURRENT_DESCRIPTION}"
        fi
        echo "==> Test result: ${TEST_RESULT}"
        echo "Tests Completed (${TEST_RESULT}): hit enter to continue"

        read -r nothing
    fi

    echo "==> Cleaning up"

    cleanup_therms "${TEST_DIR}"
    cleanup_therms_discovery "${TEST_DIR}"

    echo ""
    echo ""
    if [ "$TEST_RESULT" != "success" ]; then
        echo "==> TEST DONE: ${TEST_CURRENT_DESCRIPTION}"
    fi
    rm -rf "${TEST_DIR}"
    echo "==> Tests Removed: ${TEST_DIR}"
    echo "==> Test result: ${TEST_RESULT}"
}

TEST_CURRENT=setup
TEST_RESULT=failure

trap cleanup EXIT HUP INT TERM

# Import all the testsuites
import_subdir_files suites

# Setup test directory
TEST_DIR=$(mktemp -d -p "$(pwd)" tmp.XXX)
chmod +x "${TEST_DIR}"

THERM_DIR=$(mktemp -d -p "${TEST_DIR}" XXX)
export THERM_DIR
chmod +x "${THERM_DIR}"
spawn_therm "${TEST_DIR}" "${THERM_DIR}" 8080

run_test() {
    TEST_CURRENT=${1}
    TEST_CURRENT_DESCRIPTION=${2:-${1}}

    echo "==> TEST BEGIN: ${TEST_CURRENT_DESCRIPTION}"
    START_TIME=$(date +%s)
    ${TEST_CURRENT}
    END_TIME=$(date +%s)

    echo "==> TEST DONE: ${TEST_CURRENT_DESCRIPTION} ($((END_TIME-START_TIME))s)"
}

# allow for running a specific set of tests
if [ "$#" -gt 0 ]; then
    run_test "test_${1}"
    TEST_RESULT=success
    exit
fi

run_test test_check_deps "checking dependencies"
run_test test_check_version "checking version"
run_test test_check_tests "running unit tests"
run_test test_query "run query tests"
run_test test_config "run config tests"
run_test test_operation "run operation tests"
run_test test_schedule "run schedule tests"
run_test test_clustering_enable "run clustering enabled tests"
run_test test_clustering_membership "run clustering membership tests"
run_test test_discovery "run discovery tests"
run_test test_discovery_join "run discovery join tests"
run_test test_discovery_leave "run discovery leave tests"
run_test test_static_analysis "running static analysis"

TEST_RESULT=success
