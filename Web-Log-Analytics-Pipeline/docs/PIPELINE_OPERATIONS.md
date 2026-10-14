# Pipeline Operations Guide: Orchestration & Nightly Automation

## Milestone M7: Batch MapReduce Clickstream Log Analysis

This document specifies operational procedures, command-line interfaces, dependency ordering, metadata schemas, and scheduling conventions for the Clickstream MapReduce batch pipeline.

---

## 1. Pipeline Architecture & Job Dependency Order

The pipeline unifies MapReduce jobs J1–J6 into a deterministic batch workflow executed by `edu.bigdata.clickstream.PipelineDriver`:

```
                           Raw Clickstream Input
                       (/clickstream/raw/dt=YYYY-MM-DD)
                                   │
         ┌─────────────────────────┼─────────────────────────┐
         │ (Scan 1)                │ (Scan 2)                │ (Scan 3)
         ▼                         ▼                         ▼
   ┌──────────────┐          ┌──────────────┐          ┌──────────────┐
   │ J1: Hourly   │          │ J3: View     │          │ J5: Session  │
   │   Traffic    │          │   Count      │          │   Funnel     │
   └──────┬───────┘          └──────┬───────┘          └──────┬───────┘
          │ (traffic/)              │ (viewcounts/)           │ (session_partial/)
          ▼                         ▼                         ▼
   ┌──────────────┐          ┌──────────────┐          ┌──────────────┐
   │ J2: Peak     │          │ J4: Top-N    │          │ J6: Daily    │
   │   Hour       │          │ (Prod & Cat) │          │   Funnel     │
   └──────────────┘          └──────────────┘          └──────────────┘
     (peak/)                   (top_products/            (funnel_daily/)
                                top_categories/)
```

### Execution Stages & Output Layout

| Stage | Driver Class | Inputs | Outputs | Reducers | Combiner |
|---|---|---|---|:---:|:---:|
| **1. J1_HourlyTraffic** | `HourlyTrafficJob` | Raw clickstream partition | `<work_dir>/traffic` | 4 (configurable) | Yes |
| **2. J2_PeakHour** | `PeakHourJob` | `<work_dir>/traffic` | `<work_dir>/peak` (`peakday`, `hourofday`) | 1 | No |
| **3. J3_ViewCount** | `ViewCountJob` | Raw clickstream partition | `<work_dir>/viewcounts` (`products/`, `categories/`) | 4 | Yes |
| **4. J4_TopN_Products** | `TopNJob` | `<work_dir>/viewcounts/products` | `<work_dir>/top_products` | 1 | In-mapper TreeMap |
| **5. J4_TopN_Categories** | `TopNJob` | `<work_dir>/viewcounts/categories` | `<work_dir>/top_categories` | 1 | In-mapper TreeMap |
| **6. J5_SessionFunnel** | `SessionFunnelJob` | Raw clickstream partition | `<work_dir>/session_partial` | 8 (configurable) | Yes |
| **7. J6_DailyFunnel** | `DailyFunnelJob` | `<work_dir>/session_partial` | `<work_dir>/funnel_daily` | 1 | Yes |

---

## 2. PipelineDriver CLI Specification

### Syntax
```bash
hadoop jar target/clickstream-mr-1.0.jar edu.bigdata.clickstream.PipelineDriver \
  --input <path> \
  --work-dir <path> \
  [--run-id <id>] \
  [--queue <name>] \
  [--topn <N>] \
  [--reducers-traffic <N>] \
  [--reducers-session <N>] \
  [--force] \
  [--report-dir <path>]
```

### Arguments

| Parameter | Type | Required | Default | Description |
|---|---|:---:|---|---|
| `--input <path>` | String | **Yes** | — | HDFS or local filesystem path to the raw clickstream partition (e.g. `/clickstream/raw/dt=2019-11-01`). |
| `--work-dir <path>` | String | **Yes** | — | Root work directory for all intermediate job outputs (e.g. `/clickstream/work/daily-2019-11-01`). |
| `--run-id <id>` | String | No | Inferred | Identifier for run (inferred as `daily-YYYY-MM-DD` if input contains `dt=...`, otherwise timestamped). |
| `--queue <name>` | String | No | `batch` | YARN Capacity Scheduler queue name (`mapreduce.job.queuename`). |
| `--topn <N>` | Integer | No | `10` | Cutoff threshold for Top-N ranking. |
| `--reducers-traffic <N>` | Integer | No | `4` | Number of reducer tasks for J1 HourlyTraffic. |
| `--reducers-session <N>` | Integer | No | `8` | Number of reducer tasks for J5 SessionFunnel. |
| `--force` / `--overwrite` | Flag | No | `false` | Opt-in flag to delete and recreate work directory if it already exists. |
| `--report-dir <path>` | String | No | — | Optional target report directory recorded in metadata for M8 `export_reports.py` (PipelineDriver does not execute report exporting). |
| `--help` / `-h` | Flag | No | — | Displays usage instructions and exits with code 0. |

### Exit Codes
- **`0`**: Successful execution; all jobs passed and invariant verified (or idempotent skip of already successful run).
- **`1`**: Job failure, input/upstream output validation failure, metadata write failure, or unforced overwrite refusal.
- **`2`**: Reconciliation invariant violation (FR-MR-07).
- **`75`**: Concurrency lock collision (OS-level `FileLock` on stable lock file `parent/.<work_dir_name>.pipeline.lock` outside `work_dir`, or `flock` contention in nightly script).

---

## 3. Reconciliation Invariant Enforcement (FR-MR-07)

Immediately after J1 HourlyTraffic completes, the pipeline reconciles data records:
$$\sum (\text{J1.total} + \text{J1.malformed}) = \text{TOTAL\_RECORDS} - \text{HEADER\_SKIPPED}$$

- **`TOTAL_RECORDS`**: Total records read by mappers (counter group `ClickstreamQuality`).
- **`HEADER_SKIPPED`**: CSV header rows skipped by mappers.
- **`J1.total + J1.malformed`**: Parsed across all `part-r-*` files in `<work_dir>/traffic`.
- **Enforcement**: If the sum does not match expected, `PipelineDriver` stops immediately, marks `status: "FAILED"`, `invariant_ok: false`, writes `run_meta.json`, and exits with code 2. Downstream jobs (J2–J6) are **never executed**.

---

## 4. Run Metadata Specification (`run_meta.json`)

Written atomically to `<work_dir>/run_meta.json` using temporary file `<work_dir>/run_meta.json.tmp` and atomic filesystem rename.

### Example Schema (Successful Run)
```json
{
  "schema_version": "1.0",
  "app_version": "1.0",
  "run_id": "daily-2019-11-01",
  "status": "SUCCESS",
  "start": "2026-10-15T02:00:03Z",
  "end": "2026-10-15T02:06:44Z",
  "duration_sec": 401,
  "input": "/clickstream/raw/dt=2019-11-01",
  "work_dir": "/clickstream/work/daily-2019-11-01",
  "queue": "batch",
  "invariant_ok": true,
  "effective_config": {
    "topn": 10,
    "reducers_traffic": 4,
    "reducers_session": 8,
    "queue": "batch"
  },
  "jobs": [
    {
      "name": "J1_HourlyTraffic",
      "job_id": "job_1700000000000_0001",
      "status": "SUCCESS",
      "start": "2026-10-15T02:00:03Z",
      "end": "2026-10-15T02:01:04Z",
      "duration_sec": 61,
      "input": "/clickstream/raw/dt=2019-11-01",
      "output": "/clickstream/work/daily-2019-11-01/traffic",
      "counters": {
        "TOTAL_RECORDS": 1000001,
        "VALID_RECORDS": 985000,
        "HEADER_SKIPPED": 1,
        "MALFORMED_TIMESTAMP": 12000,
        "MALFORMED_COLUMN_COUNT": 3000
      }
    }
  ]
}
```

### Failed Run Schema
When an error or invariant violation occurs, the metadata captures diagnostic details:
```json
{
  "schema_version": "1.0",
  "app_version": "1.0",
  "run_id": "daily-2019-11-01",
  "status": "FAILED",
  "failed_job": "J1_HourlyTraffic",
  "error": "Reconciliation invariant violation: Σ(J1.total + J1.malformed) != TOTAL_RECORDS - HEADER_SKIPPED",
  "invariant_ok": false
}
```

---

## 5. Nightly Batch Automation (`scripts/nightly_batch.sh`)

### Features & Safety Guarantees
- **Shell Hygiene:** Enforces `set -euo pipefail`.
- **Mutual Exclusion (`flock`):** Acquires exclusive lock on `/tmp/clickstream_batch.lock` (override via `LOCK_FILE` environment variable or `--lock-file` flag). If locked by another run, exits immediately with code **`75`** (`EX_TEMPFAIL`) and logs `"already running"`.
- **OS-Level Persistent Concurrency Guard (`parent/.<work_dir>.pipeline.lock`):** `PipelineDriver` coordinates mutual exclusion using a stable lock file located outside the recursively cleaned `work_dir` (`parent/.<work_dir_name>.pipeline.lock`). On local and mounted POSIX/Windows filesystems, it acquires an OS kernel-level exclusive file lock (`FileLock.tryLock()`) before pre-flight checks or `--force` cleanup. The lock file is **never deleted or replaced upon normal release**, treating the file as a persistent coordination rendezvous artifact to eliminate the race condition where unlinked files are replaced by concurrent invocations. On distributed HDFS filesystems where kernel locks do not cross nodes, an atomic NameNode marker (`fs.createNewFile()`) with cryptographic owner token verification ensures cooperative exclusion and prevents foreign marker deletion.
- **Idempotent Reruns (NFR-05):** Before executing, `nightly_batch.sh` checks if `<work_dir>/run_meta.json` already records `"status": "SUCCESS"`. If so and `--force` was not requested, the run is skipped cleanly with exit code **`0`**, preserving all existing output data.
- **Safe Single Retry Strategy (FR-SCH-08):**
  - If a fresh run fails mid-pipeline (e.g. at J3 or J5), the script automatically executes **1 retry** with `--force` to clean up its own failed attempt and rerun from J1.
  - If the work directory already existed prior to script invocation and `--force` was not provided, the script **refuses to retry with `--force`**, exiting with code 1 to protect existing data from accidental destruction.
- **Pre-flight Checks:**
  - HDFS SafeMode check (`hdfs dfsadmin -safemode get`). Exits with code **`2`** if ON or unavailable.
  - YARN NodeManager check (`yarn node -list`). Exits with code **`3`** if ResourceManager is unreachable.
  - Input partition check (`hdfs dfs -test -e <partition>`). Exits with code **`4`** if input partition is absent.
- **Cron Non-Destructive Behavior:** `nightly_batch.sh --install-cron` displays the recommended `0 2 * * *` entry and instructions without modifying the user's crontab directly.
- **Log Management:** Writes detailed logs to `logs/nightly-<run_id>-<timestamp>.log` and auto-prunes logs older than 30 days (FR-SCH-09).

### Example Crontab Schedule
To execute nightly batch runs automatically every night at 02:00 UTC:
```cron
# Example crontab entry for nightly clickstream batch (02:00 UTC)
0 2 * * * /path/to/Web-Log-Analytics-Pipeline/scripts/nightly_batch.sh >> /path/to/Web-Log-Analytics-Pipeline/logs/cron.log 2>&1
```

---

## 6. Verification and Environmental Status

| Verification Scope | Status | Notes |
|---|:---:|---|
| **JUnit 5 Test Suite** | **PASS** | 201 tests passing across all packages; 0 failures. |
| **JaCoCo Line Coverage** | **PASS** | **83.32%** overall line coverage (1,659/1,991 lines), above the ≥ 80% SRS threshold. Root package `edu.bigdata.clickstream` achieves **85.92%** line coverage. |
| **Pandas Golden Baseline** | **PASS** | `verify_with_pandas.py --job all` passes all baseline invariants. |
| **Python Ingestion Suite** | **PASS** | `test_data_ingestion.py` passes 7/7 tests. |
| **Shell Test Suite** | **PASS** | `test_nightly_batch.sh` passes 8/8 tests (including lock collision exit code 75, crontab non-modification, and safe idempotency/retry). |
| **Docker Daemon / Cluster** | **PENDING** | Docker daemon remains inactive in the host environment; full distributed cluster execution remains pending cluster startup. |
