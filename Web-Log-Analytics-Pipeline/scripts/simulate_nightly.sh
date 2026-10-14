#!/usr/bin/env bash
# ==============================================================================
# simulate_nightly.sh — Replays Historical Dataset Partitions Consecutively
# Conforms to SRS §3.3.10 (FR-SCH-10)
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

POINTER_FILE="${PROJECT_ROOT}/logs/simulate_pointer.txt"
DATA_PARTITIONS_DIR="${PROJECT_ROOT}/data/partitions"

usage() {
    cat <<EOF
Usage: $(basename "$0") [options]

Simulates consecutive nightly runs by advancing a date pointer across partitions.

Options:
  --days <N>         Number of consecutive simulated days to run (default: 1)
  --reset            Reset pointer back to the earliest partition
  --help, -h         Show this message
EOF
}

DAYS=1
RESET=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --days)
            DAYS="$2"
            shift 2
            ;;
        --reset)
            RESET=true
            shift
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            echo "Error: Unknown argument '$1'" >&2
            usage
            exit 1
            ;;
    esac
done

# If reset requested, remove pointer file
if [[ "${RESET}" == true ]]; then
    rm -f "${POINTER_FILE}"
    echo "Pointer reset. Simulation will start from the earliest partition."
    exit 0
fi

mkdir -p "$(dirname "${POINTER_FILE}")"

# Find available dates
AVAILABLE_DATES=()
if [[ -d "${DATA_PARTITIONS_DIR}" ]]; then
    while IFS= read -r dir; do
        d="$(basename "${dir}" | sed 's/dt=//')"
        if [[ "${d}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
            AVAILABLE_DATES+=("${d}")
        fi
    done < <(find "${DATA_PARTITIONS_DIR}" -mindepth 1 -maxdepth 1 -type d | sort)
fi

# Fallback to standard demo dates if no partition directory found
if [[ ${#AVAILABLE_DATES[@]} -eq 0 ]]; then
    AVAILABLE_DATES=("2019-11-01" "2019-11-02" "2019-11-03")
fi

# Read pointer index
CURRENT_INDEX=0
if [[ -f "${POINTER_FILE}" ]]; then
    CURRENT_INDEX="$(cat "${POINTER_FILE}")"
fi

if [[ ${CURRENT_INDEX} -ge ${#AVAILABLE_DATES[@]} ]]; then
    echo "Simulation reached the end of available partitions (${#AVAILABLE_DATES[@]} days)."
    echo "Use --reset to restart from day 1."
    exit 0
fi

echo "Simulating ${DAYS} consecutive nightly run(s) starting from index ${CURRENT_INDEX}..."

for (( i=0; i<DAYS; i++ )); do
    if [[ ${CURRENT_INDEX} -ge ${#AVAILABLE_DATES[@]} ]]; then
        echo "All available simulation dates exhausted."
        break
    fi

    SIM_DATE="${AVAILABLE_DATES[${CURRENT_INDEX}]}"
    echo "=============================================================================="
    echo " [Simulation Day $((i+1))/${DAYS}] Date: ${SIM_DATE}"
    echo "=============================================================================="

    # Run nightly batch in simulation mode
    "${SCRIPT_DIR}/nightly_batch.sh" --date "${SIM_DATE}" --simulate --force

    CURRENT_INDEX=$((CURRENT_INDEX + 1))
    echo "${CURRENT_INDEX}" > "${POINTER_FILE}"
done

echo "Simulation completed. Current pointer: index ${CURRENT_INDEX}."
