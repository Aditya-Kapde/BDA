# Implementation Plan: Milestones M0–M9
## Project #53: Batch MapReduce Clickstream Log Analysis

**Authority Baseline:** [PRD.md](file:///d:/BDA_LAB/docs/PRD.md) · [SRS.md](file:///d:/BDA_LAB/docs/SRS.md) · [CLAUDE.md](file:///d:/BDA_LAB/docs/CLAUDE.md)  
**Execution Order:** Sequential, dependency-ordered milestones. Each milestone must pass automated verification before advancing.

---

## Milestone Overview

```
┌────────────────────────────────────────────────────────────────────────┐
│                        MILESTONE ROADMAP                               │
├────────────────────────────────────────────────────────────────────────┤
│  M0: Environment & Infrastructure Baseline (Java 11, Hadoop, Maven)   │
│  M1: Data Management, Partitioning & Fixtures                          │
│  M2: Core Parser, Custom Writables & Quality Counters                  │
│  M3: Job J1 (HourlyTraffic) & Pandas Baseline Verification            │
│  M4: Job J2 (PeakHour & MultipleOutputs)                              │
│  M5: Jobs J3 (ViewCount) & J4 (TopN Products & Categories)            │
│  M6: Jobs J5 (SessionFunnel) & J6 (DailyFunnel)                       │
│  M7: PipelineDriver & Nightly Shell Automation (flock, YARN batch)    │
│  M8: Report Exporter, Pandas Golden Check & Streamlit Dashboard        │
│  M9: Backfill, Performance Benchmarks, Final Report & Demo Script      │
└────────────────────────────────────────────────────────────────────────┘
```

---

## Detailed Milestone Specifications

### M0: Environment & Infrastructure Baseline
- **Goal:** Set up a clean, reproducible Java 11 / Maven / Hadoop 3.3.6 environment with HDFS and YARN Capacity Scheduler.
- **Tasks:**
  1. Author `pom.xml`:
     - Java 11 target.
     - Dependencies: `hadoop-client` (3.3.6, scope `provided` for cluster / `compile` for test), JUnit 5 (5.10.x), Mockito (5.x), AssertJ, SLF4J, Jackson (for `run_meta.json`).
     - Plugins: Maven Compiler Plugin, Maven Surefire/Failsafe Plugins, Maven Shade/Assembly Plugin (producing `target/clickstream-mr-1.0.jar`), JaCoCo Plugin (enforcing ≥ 80% line coverage).
  2. Create Hadoop configuration files in `conf/`:
     - `core-site.xml` (`fs.defaultFS=hdfs://hdfs-namenode:9000`).
     - `hdfs-site.xml` (`dfs.replication=1`, `dfs.blocksize=134217728`).
     - `yarn-site.xml` (CapacityScheduler enabled, memory allocations).
     - `mapred-site.xml` (`mapreduce.framework.name=yarn`, container memories).
     - `capacity-scheduler.xml` (`default` 30%, `batch` 70%, max capacity 100%).
  3. Update `docker-compose.yml`:
     - Keep `hdfs-namenode` and `hdfs-datanode`.
     - Add `yarn-resourcemanager` and `yarn-nodemanager`.
     - Retire PySpark, Airflow, and Jupyter services.
     - Add build support container or document local WSL2 build commands.
- **Verification:**
  - `mvn clean compile` succeeds with zero warnings.
  - HDFS and YARN daemons start; `hdfs dfs -ls /` returns 0; YARN RM UI shows queue `batch`.

---

### M1: Data Management, Partitioning & Fixtures
- **Goal:** Provide automated, memory-bounded ingestion, day-partitioning, and representative test fixtures.
- **Tasks:**
  1. Create `scripts/download_dataset.md`: Clear CLI (`kaggle datasets download -d mkechinov/ecommerce-behavior-data-from-multi-category-store`) and manual instructions.
  2. Implement `scripts/make_sample.sh`: Extracts the header and first 1,000,000 rows to `data/sample/sample_1m.csv`.
  3. Implement `scripts/split_by_day.py`:
     - Memory-bounded streaming CSV reader using standard library `csv.reader`.
     - Extracts UTC date from `event_time` (format `yyyy-MM-dd HH:mm:ss UTC`).
     - Appends to partition files `data/partitions/dt=YYYY-MM-DD/events.csv` with header preserved.
     - Unparseable dates stream to `data/partitions/dt=unknown/events.csv`.
     - Invariant: `rows_in == rows_out + header_lines`.
  4. Implement `scripts/ingest_to_hdfs.sh`:
     - Uploads partitions to HDFS `/clickstream/raw/dt=.../`.
     - Skips existing partitions unless `--force`.
  5. Create `src/test/resources/fixtures/mini_clickstream.csv`:
     - Contains synthetic test cases covering all 7 malformed rules, missing categories/brands, exact duplicates, midnight crossers, top-N ties, and funnel scenarios.
- **Verification:**
  - Unit test `test_split_by_day.py` passes.
  - Sample partition matches expected line count.

---

### M2: Core Parser, Custom Writables & Quality Counters
- **Goal:** Implement the authoritative parsing engine, custom Hadoop Writables, and quality tracking counters.
- **Tasks:**
  1. Implement `edu.bigdata.clickstream.common`:
     - `ClickEvent.java`: Immutable data holder with reusable setters.
     - `EventType.java`: Enum (`VIEW`, `CART`, `REMOVE_FROM_CART`, `PURCHASE`).
     - `QualityCounter.java`: Counter enum (`TOTAL_RECORDS`, `VALID_RECORDS`, `HEADER_SKIPPED`, `MALFORMED_COLUMN_COUNT`, `MALFORMED_TIMESTAMP`, `MALFORMED_EVENT_TYPE`, `MALFORMED_ID`, `MALFORMED_PRICE`, `MALFORMED_SESSION`, `MISSING_CATEGORY`, `MISSING_BRAND`).
     - `ClickEventParser.java`: Evaluates rules 1–7 in exact normative order (SRS §3.3.3). Zero regex; fast manual index splitting; non-throwing.
     - `TimeUtil.java`: Fixed-width integer extraction for timestamp parsing.
  2. Implement `edu.bigdata.clickstream.writable`:
     - `EventCountWritable.java` (`views`, `carts`, `removes`, `purchases`, `malformed`).
     - `ViewCountWritable.java` (`count`, `category`, `brand`).
     - `SessionAggWritable.java` (`minEpoch`, `flagsMask`, `removeCount`).
     - Symmetric `write()`, `readFields()`, and associative `merge()` methods.
  3. Unit Tests (`src/test/java/...`):
     - `ClickEventParserTest`: One test per malformed condition, header skipping, boundary dates, missing values.
     - Serialization tests for each custom Writable verifying byte round-trip.
- **Verification:**
  - `mvn test` passes with 100% of parser and writable tests green.
  - JaCoCo line coverage on `common` and `writable` packages ≥ 90%.

---

### M3: Job J1 (HourlyTraffic) & Initial Validation
- **Goal:** Implement and verify Job J1 (`HourlyTrafficJob`) producing Report 1 & 3A inputs.
- **Tasks:**
  1. Implement `edu.bigdata.clickstream.traffic`:
     - `HourlyTrafficMapper`: Parses line via `ClickEventParser`. Increments `QualityCounter`. Emits `Text("yyyy-MM-dd HH")` -> `EventCountWritable`.
     - `HourlyTrafficCombiner`: Sums `EventCountWritable` fields.
     - `HourlyTrafficReducer`: Sums fields, computes `total` and `error_rate = malformed / (total + malformed)`, formats tab-separated output.
     - `HourlyTrafficJob`: Configures job with 4 reducers, combiners, and queue `batch`.
  2. Implement `CombinerEquivalenceTest`:
     - Runs job with and without combiner on fixture; verifies identical output.
  3. Initial pandas baseline check:
     - Compare J1 output on `sample_1m.csv` against an independent pandas script.
- **Verification:**
  - J1 integration test passes on LocalJobRunner.
  - Pandas comparison on 1M sample yields 0 mismatches.

---

### M4: Job J2 (PeakHour & MultipleOutputs)
- **Goal:** Implement Job J2 (`PeakHourJob`) deriving peak traffic hour per day and overall hour-of-day profile.
- **Tasks:**
  1. Implement `edu.bigdata.clickstream.peak`:
     - `PeakHourMapper`: Reads J1 output. Skips `NA` rows. Emits `("D|" + date) -> ("hour:total")` and `("H|" + hour) -> ("date:total")`.
     - `PeakHourReducer`: Single reducer using `MultipleOutputs`.
       - For `D|*`: Calculates peak hour (ties broken by earliest hour) and day total; writes to named output `peakday`.
       - For `H|*`: Calculates total events and days observed; writes to named output `hourofday`.
     - `PeakHourJob`: Configures mapper, reducer, and `MultipleOutputs`.
  2. Implement unit and integration tests comparing against golden TSV.
- **Verification:**
  - Peak hour and HOD outputs match expected numbers on `mini_clickstream.csv`.

---

### M5: Jobs J3 (ViewCount) & J4 (TopN Products & Categories)
- **Goal:** Implement Report 2 analytics ranking the top-10 products and top-10 categories.
- **Tasks:**
  1. Implement Job J3 (`ViewCountJob`):
     - `ViewCountMapper`: Emits `"P|" + product_id` and `"C|" + category_code` for `view` events.
     - `ViewCountCombiner` / `Reducer`: Sums counts; tracks deterministic non-empty category/brand strings.
     - `MultipleOutputs`: Emits to `products/` and `categories/`.
  2. Implement Job J4 (`TopNJob`):
     - Parameterized by `clickstream.topn` (default 10).
     - `TopNMapper`: Maintains an in-memory bounded `TreeMap` (size N) ordered by `(views desc, key asc)`. Emits top N in `cleanup()`.
     - `TopNReducer`: Single reducer merging candidates from all mappers, writing `rank \t key \t ... \t views`.
     - Run J4 on `products/` -> `top_products/` and on `categories/` -> `top_categories/`.
- **Verification:**
  - Output files contain exactly 10 ranked products and categories.
  - Ties resolved deterministically.

---

### M6: Jobs J5 (SessionFunnel) & J6 (DailyFunnel)
- **Goal:** Implement Report 3B calculating cart abandonment, remove-from-cart rate, and session conversion.
- **Tasks:**
  1. Implement Job J5 (`SessionFunnelJob`):
     - `SessionFunnelMapper`: Key = `user_session`, Value = `SessionAggWritable` (timestamp, bitmask flags for view/cart/purchase, remove count).
     - `SessionFunnelCombiner`: Merges minEpoch (min), flags (bitwise OR), removeCount (sum).
     - `SessionFunnelReducer`: 8 reducers. Streaming merge. Assigns session to UTC date of `minEpoch`. Folds into in-memory bounded `Map<String, long[5]>`. In `cleanup()`, emits per-date partials: `date \t sessions \t cart \t purchase \t abandoned \t removes`.
  2. Implement Job J6 (`DailyFunnelJob`):
     - Single reducer consolidating per-date partials from J5 into daily funnel totals.
- **Verification:**
  - Session funnel numbers on test fixture match manual calculations exactly.
  - Zero memory buffering of individual session events.

---

### M7: PipelineDriver & Nightly Shell Automation
- **Goal:** Build the unified job runner and automated production execution wrapper.
- **Tasks:**
  1. Implement `edu.bigdata.clickstream.PipelineDriver`:
     - Extends `Configured` implements `Tool`.
     - Executes J1 -> J2, J3 -> J4 (products & categories), J5 -> J6 sequentially.
     - Reconciles invariant: `Σ(J1.total + J1.malformed) == TOTAL_RECORDS - HEADER_SKIPPED`. Fails run if invariant violated.
     - Writes `run_meta.json` with execution status, YARN app IDs, counters, timings.
  2. Implement `scripts/nightly_batch.sh`:
     - CLI flags: `--date`, `--backfill`, `--install-cron`, `--simulate`.
     - Exclusive `flock` on `/tmp/clickstream_batch.lock` (exit code 75 on collision).
     - Pre-flight checks: HDFS safemode, YARN nodes, input existence (exit codes 2, 3, 4).
     - Atomic directory staging and renaming.
     - Single retry on job failure; exits code 1 on abort.
     - Log rotation in `logs/` (prunes logs > 30 days).
  3. Implement `scripts/simulate_nightly.sh`:
     - Replays historical date partitions consecutively for demonstration.
- **Verification:**
  - Invariant test fails deliberately corrupted input.
  - Concurrent lock test verifies exit code 75.
  - End-to-end dry run succeeds.

---

### M8: Report Exporter, Pandas Golden Check & Streamlit Dashboard
- **Goal:** Decouple Hadoop from visualization by exporting 8 standardized CSV reports, verifying correctness, and rendering the 5-view dashboard.
- **Tasks:**
  1. Implement `scripts/export_reports.py`:
     - Streams HDFS outputs to `reports/<run_id>/`:
       1. `traffic_hourly.csv`
       2. `peak_hour_daily.csv`
       3. `hour_of_day_profile.csv`
       4. `top_products.csv`
       5. `top_categories.csv`
       6. `quality_daily.csv`
       7. `quality_reasons.csv`
       8. `funnel_daily.csv`
     - Writes `report_meta.json` and copies `run_meta.json`.
     - Safe mathematical formulas (zero-division produces empty cell).
     - Deterministic sorting ensures byte-identical idempotency.
  2. Implement `scripts/verify_with_pandas.py`:
     - Recomputes all 8 reports independently using pandas on `sample_1m.csv`.
     - Compares cell-by-cell against exported CSVs.
     - Prints per-check PASS/FAIL and final `ALL CHECKS PASSED`.
  3. Implement `dashboard/app.py` (Streamlit):
     - Sidebar run selector listing runs from `reports/`.
     - View 1: Overview (KPI cards + hourly line chart).
     - View 2: Peak Traffic (HOD chart, stacked event bars, peak hour table).
     - View 3: Top Pages (Top-10 products & categories, uncategorized share).
     - View 4: Errors & Funnel (Data quality error rates + abandonment/conversion funnel).
     - View 5: Job Runs (Table of YARN app IDs, counters, timings, status).
     - Download CSV buttons and metric captions.
     - Empty-state message when `reports/` is empty.
- **Verification:**
  - `verify_with_pandas.py` prints `ALL CHECKS PASSED`.
  - Streamlit dashboard loads in < 3 seconds; all 5 pages render correctly.

---

### M9: Backfill, Benchmarks, Documentation & Final Demo
- **Goal:** Execute full backfill, record performance benchmarks, compile academic documentation, and rehearse live demo.
- **Tasks:**
  1. Run backfill on dataset partitions.
  2. Record metrics in `docs/benchmarks.md`:
     - Hardware environment specifications.
     - Input sizes, record counts, and partition stats.
     - Per-job execution time and YARN resource allocations.
     - Combiner shuffle byte reduction measurements (≥ 90% target).
  3. Author `docs/final-report.md`:
     - Problem statement, architecture, dataset analysis, methodology, results, business insights, limitations.
  4. Author `docs/demo-script.md`:
     - 10-minute step-by-step evaluator walk-through (HDFS layout -> YARN UI -> trigger batch -> Streamlit dashboard -> rerun idempotency -> error injection).
- **Verification:**
  - All acceptance criteria in SRS §6.1 satisfied.
  - Final package ready for evaluation.
