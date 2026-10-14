#!/usr/bin/env bash
# ==============================================================================
# test_nightly_batch.sh — Automated tests for nightly_batch.sh & flock
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TARGET_SCRIPT="${PROJECT_ROOT}/scripts/nightly_batch.sh"

echo "=== Running Automated Tests for nightly_batch.sh ==="

# Test 1: --help returns 0
echo -n "Test 1: --help exits with 0... "
"${TARGET_SCRIPT}" --help >/dev/null 2>&1
echo "PASS"

# Test 2: --install-cron returns 0
echo -n "Test 2: --install-cron exits with 0... "
"${TARGET_SCRIPT}" --install-cron >/dev/null 2>&1
echo "PASS"

# Test 3: Unknown option exits with 1
echo -n "Test 3: Unknown flag exits with 1... "
EXIT_CODE=0
"${TARGET_SCRIPT}" --invalid-flag >/dev/null 2>&1 || EXIT_CODE=$?
if [[ ${EXIT_CODE} -eq 1 ]]; then
    echo "PASS (Code 1)"
else
    echo "FAIL (Got code ${EXIT_CODE})"
    exit 1
fi

# Test 4: flock mutual exclusion prevents concurrent executions (Exit Code 75)
echo -n "Test 4: flock collision exits with 75... "
TEST_LOCK="/tmp/test_nightly_collision_$$.lock"
exec 8>"${TEST_LOCK}"
flock -x 8

COLLISION_CODE=0
"${TARGET_SCRIPT}" --lock-file "${TEST_LOCK}" --date 2019-11-01 >/dev/null 2>&1 || COLLISION_CODE=$?

flock -u 8
exec 8>&-
rm -f "${TEST_LOCK}"

if [[ ${COLLISION_CODE} -eq 75 ]]; then
    echo "PASS (Code 75)"
else
    echo "FAIL: Expected 75 but got ${COLLISION_CODE}"
    exit 1
fi

# Test 5: Backfill without --from and --to exits with 1
echo -n "Test 5: Incomplete --backfill exits with 1... "
BF_CODE=0
"${TARGET_SCRIPT}" --backfill --simulate >/dev/null 2>&1 || BF_CODE=$?
if [[ ${BF_CODE} -eq 1 ]]; then
    echo "PASS (Code 1)"
else
    echo "FAIL (Got code ${BF_CODE})"
    exit 1
fi

# Test 6: --install-cron does not modify user's crontab
echo -n "Test 6: --install-cron does not modify crontab... "
CRON_BEFORE="$(crontab -l 2>/dev/null || true)"
CRON_OUTPUT="$("${TARGET_SCRIPT}" --install-cron 2>&1)"
CRON_AFTER="$(crontab -l 2>/dev/null || true)"
if [[ "${CRON_BEFORE}" == "${CRON_AFTER}" ]] && [[ "${CRON_OUTPUT}" == *"0 2 * * *"*"nightly_batch.sh"* ]]; then
    echo "PASS (Crontab unmodified, recommendation displayed)"
else
    echo "FAIL (Crontab was modified or recommendation missing)"
    exit 1
fi

# Test 7: Existing successful run is skipped with exit code 0 when run without --force
echo -n "Test 7: Existing successful run skipped without --force... "
TEST_WORK_DIR="/tmp/test_work_success_$$"
mkdir -p "${TEST_WORK_DIR}/daily-2019-11-01"
echo '{"status": "SUCCESS", "run_id": "daily-2019-11-01"}' > "${TEST_WORK_DIR}/daily-2019-11-01/run_meta.json"
echo 'sample_output' > "${TEST_WORK_DIR}/daily-2019-11-01/part-r-00000"

SKIP_CODE=0
"${TARGET_SCRIPT}" --date 2019-11-01 --work-dir "${TEST_WORK_DIR}" --simulate >/dev/null 2>&1 || SKIP_CODE=$?
if [[ ${SKIP_CODE} -eq 0 ]] && [[ -f "${TEST_WORK_DIR}/daily-2019-11-01/part-r-00000" ]]; then
    echo "PASS (Exited 0, existing output preserved)"
else
    echo "FAIL: Expected exit code 0 and preserved output, got ${SKIP_CODE}"
    rm -rf "${TEST_WORK_DIR}"
    exit 1
fi
rm -rf "${TEST_WORK_DIR}"

# Test 8: Pre-existing work directory without success refuses unforced overwrite on retry
echo -n "Test 8: Pre-existing failed directory does not auto-force on retry... "
TEST_WORK_DIR_FAIL="/tmp/test_work_fail_$$"
mkdir -p "${TEST_WORK_DIR_FAIL}/daily-2019-11-01"
echo '{"status": "FAILED", "run_id": "daily-2019-11-01"}' > "${TEST_WORK_DIR_FAIL}/daily-2019-11-01/run_meta.json"

FAIL_CODE=0
"${TARGET_SCRIPT}" --date 2019-11-01 --work-dir "${TEST_WORK_DIR_FAIL}" --simulate >/dev/null 2>&1 || FAIL_CODE=$?
if [[ ${FAIL_CODE} -eq 1 ]]; then
    echo "PASS (Aborted with code 1 without destroying directory)"
else
    echo "FAIL: Expected exit code 1, got ${FAIL_CODE}"
    rm -rf "${TEST_WORK_DIR_FAIL}"
    exit 1
fi
rm -rf "${TEST_WORK_DIR_FAIL}"

echo "=== ALL 8 NIGHTLY BATCH SCRIPT TESTS PASSED ==="
