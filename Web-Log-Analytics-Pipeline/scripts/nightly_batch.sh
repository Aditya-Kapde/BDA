#!/usr/bin/env bash
# ==============================================================================
# nightly_batch.sh — Production Nightly Batch Orchestrator for Clickstream MR
# Milestone M7: Pipeline Orchestration & Nightly Automation
# Conforms to SRS §3.3.10 (FR-SCH-01..FR-SCH-10) and PRD §4
# ==============================================================================
set -euo pipefail

# ------------------------------------------------------------------------------
# Default Configuration
# ------------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

LOCK_FILE="${LOCK_FILE:-/tmp/clickstream_batch.lock}"
LOG_DIR="${PROJECT_ROOT}/logs"
DEFAULT_QUEUE="batch"
JAR_PATH="${PROJECT_ROOT}/target/clickstream-mr-1.0.jar"
DRIVER_CLASS="edu.bigdata.clickstream.PipelineDriver"

DATE=""
BACKFILL=false
FROM_DATE=""
TO_DATE=""
QUEUE="${DEFAULT_QUEUE}"
INSTALL_CRON=false
SIMULATE=false
FORCE=false
TOPN=10
REDUCERS_TRAFFIC=4
REDUCERS_SESSION=8
WORK_ROOT="/clickstream/work"
RAW_ROOT="/clickstream/raw"

# ------------------------------------------------------------------------------
# Usage / Help
# ------------------------------------------------------------------------------
usage() {
    cat <<EOF
Usage: $(basename "$0") [options]

Modes:
  --date YYYY-MM-DD             Run single-day batch for specified UTC date
                                (default: yesterday UTC)
  --backfill --from D --to D    Execute backfill sequentially across date range [from, to]
  --install-cron                Display and optionally install crontab entry (0 2 * * *)
  --simulate                    Simulate run in local mode without requiring live HDFS/YARN

Options:
  --queue <name>                YARN CapacityScheduler queue (default: batch)
  --topn <N>                    Cutoff threshold for Top-N jobs (default: 10)
  --reducers-traffic <N>        Reducer count for J1 HourlyTraffic (default: 4)
  --reducers-session <N>        Reducer count for J5 SessionFunnel (default: 8)
  --work-dir <path>             Base work directory for intermediate outputs (default: /clickstream/work)
  --lock-file <path>            Custom flock lockfile path (default: /tmp/clickstream_batch.lock)
  --force                       Allow overwrite of existing work directory for this run
  --help, -h                    Show this help message

Exit Codes:
  0   Success
  1   Pipeline job execution failure / unhandled error
  2   HDFS unavailable or in SafeMode
  3   YARN ResourceManager / NodeManagers unavailable
  4   Input partition not found
  75  Lock collision: another batch execution is currently running
EOF
}

# ------------------------------------------------------------------------------
# Argument Parsing
# ------------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --date)
            DATE="$2"
            shift 2
            ;;
        --backfill)
            BACKFILL=true
            shift
            ;;
        --from)
            FROM_DATE="$2"
            shift 2
            ;;
        --to)
            TO_DATE="$2"
            shift 2
            ;;
        --queue)
            QUEUE="$2"
            shift 2
            ;;
        --install-cron)
            INSTALL_CRON=true
            shift
            ;;
        --simulate)
            SIMULATE=true
            shift
            ;;
        --force|--overwrite)
            FORCE=true
            shift
            ;;
        --topn)
            TOPN="$2"
            shift 2
            ;;
        --reducers-traffic)
            REDUCERS_TRAFFIC="$2"
            shift 2
            ;;
        --reducers-session)
            REDUCERS_SESSION="$2"
            shift 2
            ;;
        --work-dir)
            WORK_ROOT="$2"
            shift 2
            ;;
        --lock-file)
            LOCK_FILE="$2"
            shift 2
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            echo "Error: Unknown option '$1'" >&2
            usage
            exit 1
            ;;
    esac
done

# ------------------------------------------------------------------------------
# Handle --install-cron (FR-SCH-01)
# ------------------------------------------------------------------------------
if [[ "${INSTALL_CRON}" == true ]]; then
    SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
    CRON_CMD="0 2 * * * ${SCRIPT_PATH} >> ${LOG_DIR}/cron.log 2>&1"

    echo "=============================================================================="
    echo " Crontab Installation (SRS §3.3.10 FR-SCH-01)"
    echo "=============================================================================="
    echo "Recommended nightly schedule entry (runs every day at 02:00 UTC):"
    echo ""
    echo "  ${CRON_CMD}"
    echo ""
    if command -v crontab >/dev/null 2>&1; then
        echo "To install this crontab, run:"
        echo "  (crontab -l 2>/dev/null || true; echo \"${CRON_CMD}\") | crontab -"
    else
        echo "Notice: 'crontab' command not found in current environment."
        echo "Configure this entry in your Linux / WSL host scheduler."
    fi
    exit 0
fi

# ------------------------------------------------------------------------------
# Enforce Concurrency Lock via flock (FR-SCH-02)
# ------------------------------------------------------------------------------
if ! command -v flock >/dev/null 2>&1; then
    echo "Error: 'flock' command is not available." >&2
    echo "Mutual exclusion is required to prevent overlapping nightly executions." >&2
    echo "Please run in a Linux/WSL environment with util-linux installed." >&2
    exit 75
fi

# Create lockfile directory if needed
mkdir -p "$(dirname "${LOCK_FILE}")"

# Open file descriptor 9 for the lockfile
exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
    echo "Error: Another batch execution is already holding lock on ${LOCK_FILE}." >&2
    echo "Exiting with code 75 (lock collision) per FR-SCH-02." >&2
    exit 75
fi

# ------------------------------------------------------------------------------
# Setup Logging and Housekeeping (FR-SCH-09)
# ------------------------------------------------------------------------------
mkdir -p "${LOG_DIR}"

# Prune logs older than 30 days
find "${LOG_DIR}" -name "nightly-*.log" -type f -mtime +30 -delete 2>/dev/null || true

TIMESTAMP="$(date -u +'%Y%m%d-%H%M%S')"
RUN_TARGET_LABEL="${DATE:-${FROM_DATE:-default}}"
LOG_FILE="${LOG_DIR}/nightly-${RUN_TARGET_LABEL}-${TIMESTAMP}.log"

log() {
    local level="$1"
    shift
    local msg="[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] [${level}] $*"
    echo "${msg}"
    echo "${msg}" >> "${LOG_FILE}"
}

log "INFO" "Starting nightly batch orchestration script (PID: $$)"
log "INFO" "Acquired exclusive lock on ${LOCK_FILE}"
log "INFO" "Logging output to ${LOG_FILE}"

# ------------------------------------------------------------------------------
# Pre-Flight Cluster Health Checks (FR-SCH-04)
# ------------------------------------------------------------------------------
if [[ "${SIMULATE}" == false ]]; then
    # Check HDFS Safemode
    if command -v hdfs >/dev/null 2>&1; then
        log "INFO" "Checking HDFS availability and safemode status..."
        SAFEMODE_STATUS="$(hdfs dfsadmin -safemode get 2>&1 || true)"
        if [[ "${SAFEMODE_STATUS}" == *"Safe mode is ON"* ]] || [[ -z "${SAFEMODE_STATUS}" ]]; then
            log "ERROR" "HDFS is in safe mode or unavailable: ${SAFEMODE_STATUS}"
            exit 2
        fi
        log "INFO" "HDFS is available and safemode is OFF."
    else
        log "WARN" "'hdfs' command not found. Assuming local test environment."
    fi

    # Check YARN Nodes
    if command -v yarn >/dev/null 2>&1; then
        log "INFO" "Checking YARN NodeManager availability..."
        if ! yarn node -list >/dev/null 2>&1; then
            log "ERROR" "YARN ResourceManager is not reachable."
            exit 3
        fi
        ACTIVE_NODES=$(yarn node -list 2>/dev/null | grep -E "RUNNING" | wc -l || true)
        log "INFO" "YARN ResourceManager is active with ${ACTIVE_NODES} running node(s)."
    else
        log "WARN" "'yarn' command not found. Assuming local test environment."
    fi
fi

# ------------------------------------------------------------------------------
# Execution Helper Function
# ------------------------------------------------------------------------------
execute_single_day() {
    local target_date="$1"
    local run_id="daily-${target_date}"
    local input_path="${RAW_ROOT}/dt=${target_date}"
    local work_dir="${WORK_ROOT}/${run_id}"

    log "INFO" "----------------------------------------------------------------------"
    log "INFO" "Executing pipeline for run_id='${run_id}', date='${target_date}'"
    log "INFO" "Input path: ${input_path}"
    log "INFO" "Work dir  : ${work_dir}"
    log "INFO" "Queue     : ${QUEUE}"

    # Check if run already succeeded previously (Idempotent run per NFR-05)
    local meta_file="${work_dir}/run_meta.json"
    local already_succeeded=false
    if [[ -f "${meta_file}" ]] && grep -q '"status": "SUCCESS"' "${meta_file}" 2>/dev/null; then
        already_succeeded=true
    elif command -v hdfs >/dev/null 2>&1 && hdfs dfs -test -e "${meta_file}" 2>/dev/null; then
        if hdfs dfs -cat "${meta_file}" 2>/dev/null | grep -q '"status": "SUCCESS"'; then
            already_succeeded=true
        fi
    fi

    if [[ "${already_succeeded}" == true ]] && [[ "${FORCE}" == false ]]; then
        log "INFO" "Run '${run_id}' has already succeeded (status: SUCCESS). Skipping execution (use --force to re-run)."
        return 0
    elif [[ "${already_succeeded}" == true ]] && [[ "${FORCE}" == true ]]; then
        log "WARN" "Run '${run_id}' has already succeeded, but --force was specified. Overwriting run..."
    fi

    # Partition existence pre-check (FR-SCH-04)
    if [[ "${SIMULATE}" == false ]] && command -v hdfs >/dev/null 2>&1; then
        if ! hdfs dfs -test -e "${input_path}" 2>/dev/null; then
            log "ERROR" "Input partition does not exist in HDFS: ${input_path}"
            return 4
        fi
    fi

    # Check if work directory existed prior to this execution attempt
    local work_dir_existed_before=false
    if [[ -d "${work_dir}" ]] || (command -v hdfs >/dev/null 2>&1 && hdfs dfs -test -d "${work_dir}" 2>/dev/null); then
        work_dir_existed_before=true
    fi

    # Build command arguments array safely
    local cmd_args=(
        --run-id "${run_id}"
        --input "${input_path}"
        --work-dir "${work_dir}"
        --queue "${QUEUE}"
        --topn "${TOPN}"
        --reducers-traffic "${REDUCERS_TRAFFIC}"
        --reducers-session "${REDUCERS_SESSION}"
    )

    if [[ "${FORCE}" == true ]]; then
        cmd_args+=(--force)
    fi

    # Execute PipelineDriver attempt 1
    local pipeline_exit=0
    if command -v hadoop >/dev/null 2>&1 && [[ -f "${JAR_PATH}" ]]; then
        log "INFO" "Executing: hadoop jar ${JAR_PATH} ${DRIVER_CLASS} ${cmd_args[*]}"
        hadoop jar "${JAR_PATH}" "${DRIVER_CLASS}" "${cmd_args[@]}" 2>&1 | tee -a "${LOG_FILE}" || pipeline_exit=$?
    else
        log "INFO" "hadoop command or JAR not available; executing with java classpath runner"
        java -cp "${JAR_PATH}:${PROJECT_ROOT}/lib/*" "${DRIVER_CLASS}" "${cmd_args[@]}" 2>&1 | tee -a "${LOG_FILE}" || pipeline_exit=$?
    fi

    # Safe Pipeline-level single retry on failure (FR-SCH-08)
    # Safety Rule: Only retry with --force if:
    #   1. The failure was a mid-execution failure of this run, AND
    #   2. work_dir did NOT exist prior to this run (or FORCE was already explicitly true).
    # If work_dir existed beforehand and FORCE was false, attempt 1 failed due to overwrite protection;
    # retrying with --force would destructively overwrite an existing run without user consent.
    if [[ ${pipeline_exit} -ne 0 ]]; then
        if [[ ${pipeline_exit} -eq 75 ]]; then
            log "ERROR" "Execution aborted due to concurrency lock collision (exit code 75). Another pipeline process is active."
            return 75
        fi

        if [[ "${work_dir_existed_before}" == true ]] && [[ "${FORCE}" == false ]]; then
            log "ERROR" "Run failed because work directory already exists and --force was not specified."
            log "ERROR" "Refusing to automatically force overwrite pre-existing directory on retry."
            return 1
        fi

        log "WARN" "Pipeline execution failed with exit code ${pipeline_exit}. Attempting 1 automatic retry (FR-SCH-08)..."
        sleep 5
        if command -v hadoop >/dev/null 2>&1 && [[ -f "${JAR_PATH}" ]]; then
            hadoop jar "${JAR_PATH}" "${DRIVER_CLASS}" "${cmd_args[@]}" --force 2>&1 | tee -a "${LOG_FILE}" || pipeline_exit=$?
        else
            java -cp "${JAR_PATH}:${PROJECT_ROOT}/lib/*" "${DRIVER_CLASS}" "${cmd_args[@]}" --force 2>&1 | tee -a "${LOG_FILE}" || pipeline_exit=$?
        fi
    fi

    if [[ ${pipeline_exit} -eq 0 ]]; then
        log "INFO" "Run '${run_id}' completed successfully."
        return 0
    else
        log "ERROR" "Run '${run_id}' failed after retry (exit code: ${pipeline_exit})."
        return 1
    fi
}

# ------------------------------------------------------------------------------
# Dispatch: Backfill or Single Day
# ------------------------------------------------------------------------------
FINAL_EXIT=0

if [[ "${BACKFILL}" == true ]]; then
    if [[ -z "${FROM_DATE}" || -z "${TO_DATE}" ]]; then
        log "ERROR" "--backfill requires both --from YYYY-MM-DD and --to YYYY-MM-DD"
        exit 1
    fi

    log "INFO" "Beginning backfill sequence from ${FROM_DATE} to ${TO_DATE}"
    CURRENT_DATE="${FROM_DATE}"
    while [[ "${CURRENT_DATE}" < "${TO_DATE}" ]] || [[ "${CURRENT_DATE}" == "${TO_DATE}" ]]; do
        execute_single_day "${CURRENT_DATE}" || {
            FINAL_EXIT=$?
            log "ERROR" "Backfill aborted due to failure on date ${CURRENT_DATE}"
            break
        }
        # Advance date by 1 day (UTC)
        CURRENT_DATE="$(date -u -d "${CURRENT_DATE} + 1 day" +'%Y-%m-%d' 2>/dev/null || python3 -c "import datetime; d=datetime.date.fromisoformat('${CURRENT_DATE}')+datetime.timedelta(days=1); print(d.isoformat())")"
    done
else
    # Default date is yesterday UTC if not explicitly provided (FR-SCH-01)
    if [[ -z "${DATE}" ]]; then
        DATE="$(date -u -d "yesterday" +'%Y-%m-%d' 2>/dev/null || python3 -c "import datetime; print((datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(days=1)).strftime('%Y-%m-%d'))")"
        log "INFO" "No --date specified; defaulting to yesterday UTC: ${DATE}"
    fi

    execute_single_day "${DATE}" || FINAL_EXIT=$?
fi

log "INFO" "Nightly batch finished with final exit code ${FINAL_EXIT}"
exit "${FINAL_EXIT}"
