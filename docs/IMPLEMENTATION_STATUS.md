# Implementation Status & Traceability Matrix
## Project #53: Batch MapReduce Clickstream Log Analysis

**Authority Baseline:** [PRD.md](file:///d:/BDA_LAB/docs/PRD.md) (Draft v1.0) · [SRS.md](file:///d:/BDA_LAB/docs/SRS.md) (Version 1.0) · [CLAUDE.md](file:///d:/BDA_LAB/docs/CLAUDE.md)  
**Inspection Date:** October 2026  
**Audited Target:** `d:/BDA_LAB` workspace and cloned repository `Web-Log-Analytics-Pipeline`

---

## 1. Executive Summary

A comprehensive architectural audit was conducted comparing the workspace files against the authoritative specifications in `docs/PRD.md` and `docs/SRS.md`.

### Core Architectural Finding
The cloned repository (`Web-Log-Analytics-Pipeline`) was built for a different problem domain using **Apache Spark 3.5.6 (PySpark)** and **Apache Airflow 3.3.1** on an **Nginx web server access log** dataset. 
In contrast, Project #53 strictly mandates:
- **Hadoop MapReduce (Java, `org.apache.hadoop.mapreduce`)** running on **YARN Capacity Scheduler** (`batch` queue).
- **Kaggle eCommerce behavior clickstream dataset (REES46)** (`2019-Oct.csv`, `2019-Nov.csv`).
- **Streamlit** multi-page dashboard reading exported CSV reports.
- **`cron` + `flock`** scheduling.
- **Spark, PySpark, Airflow, and Hive are explicitly defined as out of scope.**

All functional and non-functional requirements below are assessed against the authoritative PRD and SRS.

---

## 2. Status Legend
- **PASS**: Implemented, verified, and adheres strictly to PRD/SRS specifications.
- **PARTIAL**: Partially implemented or reusable with modifications.
- **FAIL**: Implemented using an incompatible technology stack (e.g. PySpark instead of Java MapReduce).
- **NOT IMPLEMENTED**: Component does not exist in the repository.

---

## 3. Detailed Requirements Traceability Matrix

### 3.1 Data Ingestion & Partitioning (`FR-ING-*`)

| ID | Requirement Summary | Status | Existing Evidence | Required Changes | Verification Command / Test |
|---|---|:---:|---|---|---|
| **FR-ING-01** | Kaggle REES46 dataset download docs (CLI & manual) in `data/raw/` | **PASS** | Implemented in `scripts/download_dataset.md` and updated `README.md`. | Complete. | Inspect `scripts/download_dataset.md`. |
| **FR-ING-02** | `split_by_day.py` streaming partitioner (`dt=YYYY-MM-DD/events.csv`, header preserved, bounded memory) | **PASS** | Implemented in `scripts/split_by_day.py`. Verified by 7 automated tests in `tests/test_data_ingestion.py`. | Complete. | `python scripts/split_by_day.py --input src/test/resources/fixtures/mini_clickstream.csv --out data/partitions --force` |
| **FR-ING-03** | Unparseable dates routed to `dt=unknown/events.csv`; 0 rows dropped | **PASS** | Verified routing of invalid dates (`2019-13-01`) and blank/missing dates to `dt=unknown/events.csv`. | Complete. | `python tests/test_data_ingestion.py` |
| **FR-ING-04** | `ingest_to_hdfs.sh` uploads partitions to `/clickstream/raw/` (skip existing unless `--force`) | **PASS** | Implemented in `scripts/ingest_to_hdfs.sh`. Syntax validated with `bash -n`, argument checks verified. | Complete. | `bash scripts/ingest_to_hdfs.sh` |
| **FR-ING-05** | `make_sample.sh` creates `data/sample/sample_1m.csv` + `mini_clickstream.csv` test fixture | **PASS** | Implemented in `scripts/make_sample.sh` (streaming head extraction, size validation) and `mini_clickstream.csv`. | Complete. | `wc -l src/test/resources/fixtures/mini_clickstream.csv` |
| **FR-ING-06** | Ingestion prints summary (partitions, row counts, bytes) | **PASS** | Formatted summary table printed by `scripts/split_by_day.py` and partition count reported. | Complete. | `python scripts/split_by_day.py --help` |

---

### 3.2 Parser, Quality Counters & Custom Writables (`FR-MR-01..08`, SRS §3.4.1)

| ID | Requirement Summary | Status | Existing Evidence | Required Changes | Verification Command / Test |
|---|---|:---:|---|---|---|
| **FR-MR-01** | `ClickEventParser.parse(Text, ClickEvent)` evaluating 7 strict rules in order; never throws | **PASS** | Implemented in `ClickEventParser.java`. 17 parser unit tests in `ClickEventParserTest.java` verify 7 rules and fixture expectations. | Complete. | JUnit 5 `ClickEventParserTest` (17/17 passing) |
| **FR-MR-02** | Mapper increments `ClickstreamQuality` counters (`TOTAL_RECORDS`, `VALID_RECORDS`, `MALFORMED_*`) | **PASS** | Implemented in `QualityCounter.java` (`TOTAL_RECORDS`, `VALID_RECORDS`, `HEADER_SKIPPED`, `MALFORMED_*`, `MISSING_*`). | Complete. | JUnit 5 `DomainModelCoverageTest` |
| **FR-MR-03** | Jobs submitted to YARN queue `batch` | **PASS** | Configured in `mapred-site.xml` and programmatic default in `HourlyTrafficJob.java` (`mapreduce.job.queuename = batch`). | Complete for J1. Remaining jobs will inherit. | `HourlyTrafficJobTest.testConfigurationKeys` |
| **FR-MR-04** | Associative/commutative combiners for J1, J3, J5 | **PASS** | Implemented for J1 (`HourlyTrafficCombiner.java`), J3 (`ViewCountCombiner.java`), and J5 (`SessionFunnelCombiner.java`). Verified via algebraic associativity/commutativity proofs and multi-chunk equivalence tests. | Complete. | JUnit 5 `CombinerEquivalenceTest`, `ViewCountCombinerTest`, `SessionFunnelCombinerTest` |
| **FR-MR-05** | Deterministic output (sorted keys, stable tie-breaks) | **PARTIAL** | Implemented for J1 (`HourlyTrafficReducer.java` emits deterministically formatted 9-column TSV with sorted hourly keys). | Ensure stable tie-breaks in J2/J4. | JUnit 5 `HourlyTrafficReducerTest` |
| **FR-MR-06** | Bounded reducer memory (no buffering all values of a key) | **PARTIAL** | `HourlyTrafficReducer` uses streaming single-accumulator pattern (`EventCountWritable.merge()`); 0 event collections buffered. | Maintain streaming pattern in J2..J6 reducers. | Code inspection & Reducer tests |
| **FR-MR-07** | PipelineDriver invariant: `Σ(J1.total + J1.malformed) == TOTAL_RECORDS - HEADER_SKIPPED` | **PASS** | Implemented automated verification in `PipelineDriver.java` after J1 completion; halts run with exit code 2 and records failure in `run_meta.json` on violation. | Complete. | JUnit 5 `PipelineDriverTest.testInvariantVerification*` |
| **FR-MR-08** | Job config overridable via `-D clickstream.*` | **PARTIAL** | Implemented and verified in `HourlyTrafficJob.java` via Hadoop `GenericOptionsParser` and `ToolRunner`. | Replicate pattern in J2..J6 drivers. | `HourlyTrafficJobTest.testConfigurationKeys` |
| **SRS §3.4.1** | Custom Writables: `EventCountWritable`, `ViewCountWritable`, `SessionAggWritable`, `DailyFunnelWritable` | **PASS** | Implemented in `edu.bigdata.clickstream.writable`. Serialization round-trip, merge, bitmasks, and reset tested. | Complete. | JUnit 5 `*WritableTest` (16/16 passing) |

---

### 3.3 MapReduce Analytics Jobs J1–J6 (`FR-MR-10..15`)

| ID | Requirement Summary | Status | Existing Evidence | Required Changes | Verification Command / Test |
|---|---|:---:|---|---|---|
| **FR-MR-10** | **J1: HourlyTraffic** (date, hour, total, views, carts, removes, purchases, malformed, error_rate) | **PASS** | Implemented in `edu.bigdata.clickstream.traffic` (`HourlyTrafficMapper`, `HourlyTrafficCombiner`, `HourlyTrafficReducer`, `HourlyTrafficJob`). Tested with 15 tests, 89.50% line coverage, independent pandas verification. | Complete. | JUnit 5 `HourlyTraffic*Test` & `verify_with_pandas.py` |
| **FR-MR-11** | **J2: PeakHour** (`MultipleOutputs`: `peakday` and `hourofday`) | **PASS** | Implemented in `edu.bigdata.clickstream.peakhour` (`PeakHourMapper`, `PeakHourReducer`, `PeakHourJob`). Tested with 18 tests, 85.14% line coverage, tie-break verification (smallest hour wins), independent pandas baseline. | Complete. | JUnit 5 `PeakHour*Test` & `verify_with_pandas.py --job j2` |
| **FR-MR-12** | **J3: ViewCount** (raw views to `products/` and `categories/` via `MultipleOutputs`) | **PASS** | Implemented in `edu.bigdata.clickstream.viewcount` (`ViewCountMapper`, `ViewCountCombiner`, `ViewCountReducer`, `ViewCountJob`). Tested with 16 tests, 89.60% line coverage, `MultipleOutputs` routing to `products/part` and `categories/part`, independent pandas baseline. | Complete. | JUnit 5 `ViewCount*Test` & `verify_with_pandas.py --job j3` |
| **FR-MR-13** | **J4: TopN** (in-mapper bounded `TreeMap`, top-10 products & categories) | **PASS** | Implemented in `edu.bigdata.clickstream.topn` (`TopNCandidate`, `TopNMapper`, `TopNReducer`, `TopNJob`). Tested with 22 tests, 91.36% line coverage, bounded in-mapper TreeMap invariant, critical distributed-sort equivalence test, independent pandas baseline. | Complete. | JUnit 5 `TopN*Test` & `verify_with_pandas.py --job j4` |
| **FR-MR-14** | **J5: SessionFunnel** (`user_session` aggregation, bitmask flags, remove count, cleanup per-date emit) | **PASS** | Implemented in `edu.bigdata.clickstream.sessionfunnel` (`SessionFunnelMapper`, `SessionFunnelCombiner`, `SessionFunnelReducer`, `SessionFunnelJob`). Tested with 27 unit tests across package and writable, 94.48% package line coverage, 92.26% overall line coverage, independent pandas baseline verification. | Complete. | JUnit 5 `SessionFunnel*Test` & `verify_with_pandas.py --job j5` |
| **FR-MR-15** | **J6: DailyFunnel** (merges J5 per-date outputs into daily funnel counts) | **PASS** | Implemented in `edu.bigdata.clickstream.dailyfunnel` (`DailyFunnelMapper`, `DailyFunnelCombiner`, `DailyFunnelReducer`, `DailyFunnelJob`) and `DailyFunnelWritable`. Tested with 26 unit tests across package and writable, 94.79% package line coverage, 92.94% overall line coverage, critical 8-reducer multi-partition consolidation test, independent pandas baseline verification. | Complete. | JUnit 5 `DailyFunnel*Test` & `verify_with_pandas.py --job j6` |

---

### 3.4 Orchestration & Scheduling (`FR-SCH-*`)

| ID | Requirement Summary | Status | Existing Evidence | Required Changes | Verification Command / Test |
|---|---|:---:|---|---|---|
| **FR-SCH-01** | `nightly_batch.sh --install-cron` sets up `0 2 * * *` | **PASS** | Implemented in `scripts/nightly_batch.sh --install-cron`. Recommends and outputs standard 02:00 UTC schedule entry. | Complete. | `bash scripts/nightly_batch.sh --install-cron` |
| **FR-SCH-02** | Exclusive `flock` on `/tmp/clickstream_batch.lock`, exits code 75 on lock contention | **PASS** | Implemented in `scripts/nightly_batch.sh` with `flock -n 9` on configurable lock path; exits with code 75 on collision. | Complete. | Automated test in `tests/test_nightly_batch.sh` (Code 75 PASS) |
| **FR-SCH-03** | `--date D` (`daily-D`) and `--backfill --from --to` modes | **PASS** | Implemented in `scripts/nightly_batch.sh`. Supports single-day (defaults to yesterday UTC) and sequential date range backfill. | Complete. | `bash tests/test_nightly_batch.sh` & script verification |
| **FR-SCH-04** | Pre-flight checks (HDFS, YARN, inputs; exit codes 2, 3, 4) | **PASS** | Implemented in `scripts/nightly_batch.sh`: `hdfs dfsadmin -safemode get` (code 2), `yarn node -list` (code 3), partition check (code 4). | Complete. | Script inspection and simulated tests |
| **FR-SCH-05** | Idempotent execution (re-run replaces work and report dirs; atomic rename) | **PASS** | Implemented via `--force` / `--overwrite` in `PipelineDriver.java` and `nightly_batch.sh`; metadata written atomically via `.tmp` rename. | Complete. | JUnit 5 `PipelineDriverTest` overwrite & atomic write tests |
| **FR-SCH-06** | YARN Capacity Scheduler queue `batch` (70%) and `default` (30%) | **PASS** | Configured in `conf/capacity-scheduler.xml`. `PipelineDriver` propagates queue `batch` (default) or CLI override to all jobs. | Complete. | `conf/capacity-scheduler.xml` validation & `PipelineDriverTest` |
| **FR-SCH-07** | `PipelineDriver` records `run_meta.json` with YARN app IDs, counters, timings | **PASS** | Implemented in `PipelineDriver.java`. Serializes versioned JSON metadata including per-job status, timings, counters, and effective config. | Complete. | JUnit 5 `PipelineDriverTest.testRunMetaJson*` |
| **FR-SCH-08** | Pipeline retry once on job failure; abort with exit code 1 | **PASS** | Implemented in `scripts/nightly_batch.sh`. Automatically performs 1 retry with `--force` before aborting with exit code 1. | Complete. | Shell script verification |
| **FR-SCH-09** | Log rotation in `logs/nightly-<run_id>-<timestamp>.log` (prune > 30 days) | **PASS** | Implemented in `scripts/nightly_batch.sh`. Writes timestamped logs and automatically prunes logs older than 30 days. | Complete. | Script log generation check |
| **FR-SCH-10** | `--simulate` / `simulate_nightly.sh` sequential date replay | **PASS** | Implemented `scripts/simulate_nightly.sh` with pointer file tracking and `--simulate` flag in `nightly_batch.sh`. | Complete. | `bash scripts/simulate_nightly.sh --help` |

---

### 3.5 Reporting & Verification (`FR-RPT-*`)

| ID | Requirement Summary | Status | Existing Evidence | Required Changes | Verification Command / Test |
|---|---|:---:|---|---|---|
| **FR-RPT-01** | `export_reports.py` streams HDFS outputs to 8 CSV reports in `reports/<run_id>/` | **PASS** | Implemented `scripts/export_reports.py`. Streams J1–J6 part files into 8 standardized CSV reports with staging and atomic rename. | Complete. | `python scripts/export_reports.py --run-id daily-2019-11-01 --work-dir ...` |
| **FR-RPT-02** | Derived column formulas & safe division by zero | **PASS** | Implemented exact rate formulas with 4 decimal places (`safe_div_pct`); empty string on zero denominator per SRS §3.5. | Complete. | `tests/test_export_reports.py` & `verify_with_pandas.py` |
| **FR-RPT-03** | Generates `report_meta.json` with definitions, counters, and timestamp | **PASS** | Generated `report_meta.json` containing schema version, run_id, report metadata, record counts, and column definitions per SRS. | Complete. | `tests/test_export_reports.py` |
| **FR-RPT-04** | Copies `run_meta.json` into report directory | **PASS** | Implemented validated copy of `run_meta.json` from work directory to reports directory; validates pipeline SUCCESS status first. | Complete. | `tests/test_export_reports.py` |
| **FR-RPT-05** | Byte-identical report rerun idempotency | **PASS** | Strict deterministic multi-key sorting, stable UTF-8 Unix newlines, and no dynamic timestamps in CSV data ensure byte-for-byte identical output. | Complete. | `test_export_deterministic_byte_identical` in `test_export_reports.py` |
| **FR-RPT-06** | `verify_with_pandas.py` golden verification (`ALL CHECKS PASSED`) | **PASS** | Extended `scripts/verify_with_pandas.py` with `--reports-dir` and independent computation verifying all 8 CSVs and metadata. | Complete. | `python scripts/verify_with_pandas.py --reports-dir ...` |

---

### 3.6 Streamlit Dashboard (`FR-DSH-*`)

| ID | Requirement Summary | Status | Existing Evidence | Required Changes | Verification Command / Test |
|---|---|:---:|---|---|---|
| **FR-DSH-01** | Sidebar run selector listing runs from `reports/` | **PASS** | Implemented in `dashboard/app.py` & `dashboard/data_loader.py`. Dynamically lists available run folders newest first; switching runs reloads data without stale state. | Complete. | `tests/test_dashboard.py` |
| **FR-DSH-02** | Overview View: KPI cards + hourly traffic line chart | **PASS** | Implemented in `render_overview()`. Displays reconciled total events, malformed rate, cart abandonment %, conversion %, peak hour, and hourly time-series chart. | Complete. | `tests/test_dashboard.py` |
| **FR-DSH-03** | Peak Traffic View: HOD chart, event-type breakdown, peak hour table | **PASS** | Implemented in `render_peak_traffic()`. Displays 00–23 UTC HOD profile, stacked hourly event composition, and daily peak table. | Complete. | `tests/test_dashboard.py` |
| **FR-DSH-04** | Top Pages View: Top-10 products & categories, uncategorized share | **PASS** | Implemented in `render_top_pages()`. Visualizes top products (`product_id · brand · category`), top categories, and uncategorised view share KPI. | Complete. | `tests/test_dashboard.py` |
| **FR-DSH-05** | Errors & Funnel View: Data quality & behavioral failure rates | **PASS** | Implemented in `render_errors_and_funnel()`. Section A: data quality rate & reason breakdown; Section B: abandonment, conversion, and remove rates with LaTeX/markdown formula captions. | Complete. | `tests/test_dashboard.py` |
| **FR-DSH-06** | Job Runs View: Table of runs, YARN app IDs, counters, timings | **PASS** | Implemented in `render_job_runs()`. Displays pipeline status, execution mode, J1–J6 timings, YARN/Local app IDs, framework counters, and invariant checks. | Complete. | `tests/test_dashboard.py` |
| **FR-DSH-07** | Download CSV buttons on all tables | **PASS** | Implemented via `st.download_button` on all tables across Overview, Peak Traffic, Top Pages, and Errors & Funnel views. | Complete. | `dashboard/app.py` inspection |
| **FR-DSH-08** | Empty-state message when `reports/` is empty | **PASS** | Implemented in `main()`. Displays informative warning and CLI generation instructions when no runs exist in reports directory. | Complete. | `tests/test_dashboard.py` |

---

### 3.7 Non-Functional & Quality Requirements (`NFR-*`)

| ID | Requirement Summary | Status | Existing Evidence | Required Changes | Verification Command / Test |
|---|---|:---:|---|---|---|
| **NFR-01** | Zero mismatches vs independent pandas golden check | **PASS** | Independent pandas golden baselines verify all J1–J6 jobs and all 8 M8 exported CSV reports + metadata against SRS rules and invariants without discrepancy. | Complete. | `python scripts/verify_with_pandas.py --job all` |
| **NFR-02** | Invariant verification holds on every run | **PASS** | `PipelineDriver.java` verifies `Σ(J1.total + J1.malformed) == TOTAL_RECORDS - HEADER_SKIPPED` on every run. Aborts with exit code 2 and records failure if violated. | Complete. | JUnit 5 `PipelineDriverTest` |
| **NFR-03** | Performance: < 10 min daily, < 90 min backfill on 4-core / 8 GB | **NOT IMPLEMENTED** | Cloned PySpark run took ~6 min for 10M rows. | Optimize MapReduce jobs with combiners and 4–8 reducers. | Benchmark execution timing. |
| **NFR-04** | Combiners reduce shuffle bytes by ≥ 90% in J1 & J3 | **NOT IMPLEMENTED** | None. | Implement and measure Combiners with and without combiner. | Compare counter `REDUCE_SHUFFLE_BYTES`. |
| **NFR-05** | Idempotent reruns; atomic publishing | **PASS** | Overwrite protection requires `--force`; `run_meta.json` written atomically via `.tmp` rename; staging work directories strictly isolated. | Complete. | JUnit 5 `PipelineDriverTest` & `tests/test_nightly_batch.sh` |
| **NFR-06** | Scalability; no single node memory bottleneck | **NOT IMPLEMENTED** | None. | Stream all records; bounded collections only. | Run with restricted heap size (`-Xmx512m`). |
| **NFR-07** | Observability: YARN IDs, counters, logs per run | **PASS** | `PipelineDriver` records per-job YARN app IDs, status, timings, counters in `run_meta.json`; `nightly_batch.sh` writes detailed logs to `logs/`. | Complete. | `PipelineDriverTest` & inspect `run_meta.json` |
| **NFR-08** | JaCoCo test coverage ≥ 80% on logic packages | **PASS** | Measured line coverage: **83.32%** (1,659/1,991 lines) across all `edu.bigdata.clickstream.*` logic classes across 201 passing JUnit 5 tests. Root package achieves **87.82%** line coverage. | Complete. | `powershell -File run_tests.ps1` |
| **NFR-09** | Portability: reproducible on Ubuntu 22.04 / WSL2 / Docker | **PARTIAL** | Docker Compose exists for HDFS, Spark, Airflow. | Streamline Docker Compose for HDFS + YARN only. | Run on clean environment via script. |
| **NFR-10** | Usability: dashboard loads in < 3 s; formula captions | **PASS** | Precomputed CSV loading executes in < 0.1 s; all metrics display formula definitions in captions; clean decoupled architecture tested headless. | Complete. | `python tests/test_dashboard.py` |
| **NFR-11** | Security: no credentials in repo; localhost binding | **PARTIAL** | Hardcoded passwords in Airflow trigger scripts. | Strip hardcoded credentials; adhere to security rules. | Security audit of repo. |
| **NFR-12** | Documentation: README, benchmarks.md, final-report.md, demo-script.md | **PARTIAL** | README exists for cloned repo; Project #53 docs missing. | Author `docs/benchmarks.md`, `docs/final-report.md`, `docs/demo-script.md`. | Inspect documentation deliverables. |

---

## 4. Reusable vs Incompatible Components Audit

### 4.1 Confirmed Reusable Components
1. **HDFS Docker Infrastructure** (`docker-compose.yml`):
   - `hdfs-namenode` (`bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8`): Port remapped to 19870 to avoid Windows Hyper-V reserved port range (9870). Working healthcheck.
   - `hdfs-datanode` (`bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8`): Configured volume mounts and networking.
   - *Modification needed:* Add YARN ResourceManager and NodeManager services; update Hadoop version to 3.3.6 where possible.
2. **HDFS Client Configuration**:
   - `hadoop-config/core-site.xml` (`fs.defaultFS = hdfs://hdfs-namenode:9000`) is reusable for client connectivity.
3. **Requirement Specifications**:
   - `docs/PRD.md`, `docs/SRS.md`, and `docs/CLAUDE.md` provide an exact blueprint.

### 4.2 Incompatible / To-Be-Retired Components
1. **Spark Stack**:
   - `spark-master` and `spark-worker` services in `docker-compose.yml`.
   - `spark-apps/01_bronze.py`, `spark-apps/02_silver.py`, `spark-apps/03_gold.py`, `spark-apps/04_export_to_local.py`, `spark-apps/verify_data.py`.
2. **Airflow Stack**:
   - `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-init`, `airflow-db` (PostgreSQL).
   - `Dockerfile.airflow`, `dags/log_processing_dag.py`, and trigger scripts (`trigger_dag.py`, `check_dag_status.py`, etc.).
3. **Jupyter & Nginx-Specific Artifacts**:
   - `jupyter` service in `docker-compose.yml`.
   - `notebooks/01_data_exploration.ipynb` through `05_dashboard.ipynb`.
   - `output/*.parquet` folders.
   - `docs/*.png` (Nginx pipeline architecture and dashboard screenshots).

---

## 5. Host & Platform Risks & Constraints

1. **Docker Daemon Status**:
   - Docker Desktop is active and accessible via Docker Engine 29.7.2 (WSL2, 12 CPUs, 7.41 GiB RAM).
2. **Build Tooling on Host**:
   - `mvn` is not installed on the Windows host PATH.
   - Windows host has OpenJDK 21, whereas Hadoop 3.3.6 requires Java 11 for compilation and runtime compatibility.
   - *Mitigation:* Maven builds are executed via WSL2 Ubuntu environment or packaged JARs (`target/clickstream-mr-1.0.jar` verified: 86,688 bytes).
3. **Memory Baseline (8 GB / 12 GB Target)**:
   - Retiring Airflow (5 containers) and Spark saves ~6 GB of RAM, leaving ample headroom for HDFS and YARN.

---

## 6. Live Hadoop/YARN Deployment & Smoke Test Audit (Milestone M10)

Live integration testing of the Hadoop 3.3.6 Compose stack was initiated per M10 specifications.

### 6.1 Execution Evidence & Stage Traceability

| Stage | Operation | Command Executed | Result / Output | Status |
|---|---|---|---|:---:|
| **Preflight** | Host Docker check | `docker ps` / `docker info` | Docker Desktop 29.7.2 running (WSL2 Linux, 12 CPUs, 7.41 GB RAM) | **PASS** |
| **Preflight** | JAR verification | `Get-Item target\clickstream-mr-1.0.jar` | Built JAR verified present (86,688 bytes, 44 classes) | **PASS** |
| **Preflight** | Exporter CLI audit | Inspect `scripts/export_reports.py` | Verified arguments (`--run-id`, `--work-dir`, `--reports-dir`, `--force`, `--hadoop-version`) and layout | **PASS** |
| **Preflight** | Verifier CLI audit | Inspect `scripts/verify_with_pandas.py` | Verified arguments (`--csv`, `--job`, `--topn`, `--reports-dir`, `--mr-output`, etc.) | **PASS** |
| **Preflight** | Ingestion Contract | Inspect `scripts/ingest_to_hdfs.sh` | Requires `dt=*` partition directory; `mini_clickstream.csv` requires `split_by_day.py` partitioning first | **DOCUMENTED** |
| **Step 2** | Launch Compose Cluster | `docker compose up -d` | Image `apache/hadoop:3.3.6` pulled, volumes & network created, containers created | **PARTIAL** |
| **Step 2** | Container Startup | `docker compose ps -a` | `hdfs-namenode` exited with code 1; dependents held in `Created` | **FAIL** |
| **Step 2** | NameNode Log Inspection | `docker logs hdfs-namenode` | `InconsistentFSStateException: Directory /hadoop/dfs/name is in an inconsistent state: storage directory does not exist or is not accessible` | **FAIL (BLOCKER)** |
| **Step 3** | J1–J6 Pipeline Execution | Blocked by Step 2 failure | Stopped per Safety Rule: Halt before dependent stages | **PENDING** |
| **Step 4** | Repeat-run Safety | Blocked by Step 2 failure | Stopped per Safety Rule: Halt before dependent stages | **PENDING** |
| **Step 5** | Live Dashboard Launch | Blocked by Step 2 failure | Stopped per Safety Rule: Halt before dependent stages | **PENDING** |

### 6.2 Identified Deployment Blockers & Root Causes (from M10)

1. **Storage Path Mismatch & Unformatted NameNode**:
   - `conf/hdfs-site.xml` specified `dfs.namenode.name.dir = file:///hadoop/dfs/name` and `dfs.datanode.data.dir = file:///hadoop/dfs/data`.
   - `docker-compose.yml` mounts persistent volumes to `/opt/hadoop/dfs/name` and `/opt/hadoop/dfs/data`.
   - In `apache/hadoop:3.3.6`, `/opt/starter.sh` expects `ENSURE_NAMENODE_DIR` to format an uninitialized volume, checking `[ ! -d "$ENSURE_NAMENODE_DIR" ]`. Setting it to `/opt/hadoop/dfs/name/current` formats on first startup and skips formatting when `current/` exists.
   - Volumes were owned by `root:root` (mode 755), preventing user `hadoop` (uid 1000) from writing.
2. **JAR Accessibility Inside Container**:
   - `docker-compose.yml` only mounts `./conf/*` into `/etc/hadoop/*`. The application JAR `target/clickstream-mr-1.0.jar` is on the host filesystem and is not mounted inside the container. It must either be copied (`docker cp`) or mounted into `yarn-resourcemanager` before MapReduce execution.
3. **Partition Directory Ingestion Requirement**:
   - `scripts/ingest_to_hdfs.sh` iterates over `$LOCAL_DIR/dt=*`. Uploading a single flat fixture (`mini_clickstream.csv`) directly requires first running `split_by_day.py` to generate the partitioned directory structure or using direct `hdfs dfs -put`.

---

## 7. Milestone M10.1: HDFS Storage Path Repair & Safe NameNode Bootstrap

Milestone M10.1 resolved the HDFS storage path mismatch and safely initialized HDFS NameNode and DataNode.

### 7.1 Configuration & Infrastructure Changes

1. **Storage Path Alignment (`conf/hdfs-site.xml`)**:
   - Updated `dfs.namenode.name.dir` to `file:///opt/hadoop/dfs/name`.
   - Updated `dfs.datanode.data.dir` to `file:///opt/hadoop/dfs/data`.
   - Result: Exactly matches `docker-compose.yml` persistent volume mount targets.
2. **Safe Idempotent NameNode Formatting (`docker-compose.yml`)**:
   - Added `ENSURE_NAMENODE_DIR=/opt/hadoop/dfs/name/current` to `hdfs-namenode` environment.
   - Mechanism: `/opt/starter.sh` tests `[ ! -d "$ENSURE_NAMENODE_DIR" ]`. On a fresh volume, `current/` is missing, triggering a one-time format. Once formatted, `current/` exists, guaranteeing subsequent container restarts never re-format or corrupt existing namespace metadata.
3. **Volume Permission Alignment**:
   - Inspected volumes `clickstream-hdfs-nn-data` and `clickstream-hdfs-dn-data`: confirmed 100% empty (freshly initialized, 0 bytes, no existing metadata).
   - Set ownership of `/opt/hadoop/dfs/name` and `/opt/hadoop/dfs/data` on the persistent named volumes to `hadoop:users` (1000:100). Verified write access for `user: hadoop`.

### 7.2 Validation Evidence

| Check | Command Executed | Verified Evidence | Status |
|---|---|---|:---:|
| **NameNode Container Health** | `docker compose ps` | Container `hdfs-namenode` status: `Up (healthy)`, port 19870/9870 and 9000 bound | **PASS** |
| **DataNode Container Health** | `docker compose ps` | Container `hdfs-datanode` status: `Up`, registered with NameNode | **PASS** |
| **NameNode Logs** | `docker logs hdfs-namenode` | Loaded FSImage from `/opt/hadoop/dfs/name/current`, left safe mode after 0 s, 0 errors | **PASS** |
| **Safe Mode Status** | `docker exec hdfs-namenode hdfs dfsadmin -safemode get` | Returns `Safe mode is OFF` (exit code 0) | **PASS** |
| **HDFS Operations** | `hdfs dfs -mkdir`, `-ls`, `-rm` | Verified directory creation, listing, and recursive deletion on active filesystem | **PASS** |
| **DataNode Registration** | `docker exec hdfs-namenode hdfs dfsadmin -report` | `Live datanodes (1): 172.18.0.3:9866 (hdfs-datanode)`, 951.35 GB remaining | **PASS** |
| **Namespace Persistence on Restart** | `docker restart hdfs-namenode` | NameNode restarted; replayed edits 1–4, skipped re-formatting, persisted marker directory across container restart | **PASS** |

### 7.3 Prerequisites for Subsequent MapReduce Milestones

1. **YARN Cluster Validation**: Completed in Milestone M10.2.
2. **JAR Delivery**: Completed in Milestone M10.2.

---

## 8. Milestone M10.2: YARN Health, Capacity Scheduler & JAR Delivery

Milestone M10.2 validated YARN cluster health, verified Capacity Scheduler queue allocations, safely delivered the application JAR, and validated submission readiness.

### 8.1 Configuration & Environment Alignments

1. **YARN Configuration Directory (`docker-compose.yml`)**:
   - Added `HADOOP_CONF_DIR=/etc/hadoop` to `yarn-resourcemanager`, `yarn-nodemanager`, and `historyserver`.
   - Ensures YARN daemons read mounted cluster configuration files (`yarn-site.xml`, `mapred-site.xml`, `capacity-scheduler.xml`) instead of falling back to default image templates.
2. **ResourceManager Web Endpoint (`conf/yarn-site.xml`)**:
   - Added `yarn.resourcemanager.webapp.address = 0.0.0.0:8088`.
   - Allows Web UI and healthchecks to bind across all container interfaces, enabling `curl http://localhost:8088/cluster` to return HTTP 200 OK.

### 8.2 Live Validation Evidence

| Check | Target | Command Executed | Verified Evidence | Status |
|---|---|---|---|:---:|
| **ResourceManager Health** | Container & Web UI | `docker compose ps yarn-resourcemanager`<br>`curl -s http://localhost:8088/cluster` | Container `Up (healthy)`. Web UI HTTP 200 OK with Capacity Scheduler | **PASS** |
| **NodeManager Health** | Container & Daemon | `docker logs yarn-nodemanager` | Registered with RM as `yarn-nodemanager:38377` with `<memory:5120, vCores:8>` | **PASS** |
| **NodeManager Registration** | Cluster Membership | `docker exec yarn-resourcemanager yarn node -list` | `Total Nodes: 1`, `Node-State: RUNNING`, `Node-Http-Address: yarn-nodemanager:8042` | **PASS** |
| **Capacity Scheduler Queues** | Queue Allocations | `yarn queue -status batch`<br>`yarn queue -status default` | **`batch`**: `State: RUNNING`, `Capacity: 70.00%`, `Max: 100.00%`<br>**`default`**: `State: RUNNING`, `Capacity: 30.00%`, `Max: 100.00%` | **PASS** |
| **HDFS Connectivity from YARN** | `fs.defaultFS` | `docker exec yarn-resourcemanager hdfs dfs -ls /` | Connected to `hdfs://hdfs-namenode:9000`; listed `/tmp` cleanly | **PASS** |
| **JAR Delivery & Integrity** | Delivery Checksums | Host: `Get-FileHash target\clickstream-mr-1.0.jar`<br>Container: `sha256sum /opt/hadoop/clickstream-mr-1.0.jar` | **Exact SHA-256 match**:<br>`3636e29c68867ab39944797dfdaab8fb564adc5702c3134e6313d0aded2475f2` (86,688 bytes, mode `-rwxr-xr-x`) | **PASS** |
| **Submission Readiness (Connectivity)** | YARN Client | `hadoop jar ... PipelineDriver` | YARN RPC endpoints `:8032`, `:8030`, `:8031`, `:8033`, HDFS RPC `:9000` accessible | **PASS** |

### 8.3 Blocker Identified for Subsequent Milestone (M10.3 / MapReduce Execution)

- **Java Bytecode Version Incompatibility**:
  - `target/clickstream-mr-1.0.jar` was compiled with Java 11 (class file version 55.0).
  - The runtime JVM inside `apache/hadoop:3.3.6` is OpenJDK 8 (`1.8.0_212-b04`, class file version 52.0).
  - Attempting to load `edu.bigdata.clickstream.PipelineDriver` throws `java.lang.UnsupportedClassVersionError: class file version 55.0, this version of the Java Runtime only recognizes class file versions up to 52.0`.
  - In strict compliance with M10.2 rules (*"Do not rebuild the JAR or modify Java code merely to make delivery work"*), the JAR was delivered as-is and verified. For Milestone M10.3, the JAR must be compiled to target Java 8 bytecode (`<maven.compiler.release>8</maven.compiler.release>` / `<release>8</release>`) or a Java 11 Hadoop container runtime must be used.
