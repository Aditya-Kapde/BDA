# SRS — Batch MapReduce Clickstream Log Analysis

**Version 1.0 · Software Requirements Specification** (structured after IEEE 830 / ISO 29148)
Authoritative technical spec. `PRD.md` explains why; `CLAUDE.md` explains how to work in the repo.

---

## 1. Introduction

### 1.1 Purpose
Specify the behaviour, interfaces, data, and quality attributes of a batch system that stores e-commerce clickstream logs on HDFS and produces peak-traffic-hour, top-10-page, and error-rate reports via Hadoop MapReduce jobs scheduled nightly on YARN, with a batch report dashboard.

### 1.2 Scope
In scope: ingestion, MapReduce analytics (J1–J6), nightly scheduling, report export, dashboard, tests, benchmarks. Out of scope: streaming, Spark/Hive/Pig, ML, authentication, alerting, time-zone conversion.

### 1.3 Definitions

| Term | Meaning |
|---|---|
| Event | One CSV row of the dataset |
| Valid / malformed record | Passes / fails the rules in §3.3.3 |
| Business day | UTC date of `event_time` |
| Partition | HDFS directory `dt=YYYY-MM-DD` holding one day's events |
| Run / run_id | One pipeline execution and its identifier (`daily-…`, `backfill-…`) |
| Session | Set of events sharing a `user_session` value |
| Abandoned session | Session with ≥ 1 `cart` event and 0 `purchase` events |
| Page | Product page identified by `product_id` (counts `view` events only) |
| HOD profile | Hour-of-day profile (hours 0–23) |

### 1.4 References
Kaggle dataset *eCommerce behavior data from multi-category store* (REES46); Apache Hadoop 3.3.6 documentation (HDFS, MapReduce, YARN Capacity Scheduler); `PRD.md`; `CLAUDE.md`.

### 1.5 Document conventions
Requirement IDs: `FR-<area>-<nn>` functional, `NFR-<nn>` non-functional. Priority: **M**ust, **S**hould, **C**ould. "Shall" = mandatory.

---

## 2. Overall description

### 2.1 Product perspective
Standalone batch analytics system composed of: (1) ingestion scripts, (2) a Hadoop cluster (HDFS + YARN), (3) a Java MapReduce application jar, (4) a scheduler wrapper, (5) an exporter, (6) a Streamlit dashboard. The dashboard is decoupled from the cluster through exported files in `reports/`.

```
Kaggle CSV → split_by_day.py → HDFS raw/dt=…
cron → nightly_batch.sh → PipelineDriver → YARN(batch queue) → J1…J6 → HDFS work/<run_id>
export_reports.py → reports/<run_id>/*.csv, report_meta.json, run_meta.json → dashboard
```

### 2.2 Product functions (summary)
Ingest & partition · validate & count · aggregate hourly traffic · derive peak hours · rank top pages/categories · compute session funnel & error rates · schedule nightly · export · visualise · record run metadata.

### 2.3 User classes
Business Analyst (dashboard consumer), Data/Platform Engineer (operates pipeline), Course Evaluator (verifies). See PRD §3.

### 2.4 Operating environment
Ubuntu 22.04 (native/VM/WSL2) or Docker Compose; OpenJDK 11; Hadoop 3.3.6 pseudo-distributed (optionally 3 nodes); Maven 3.9; Python 3.10+; modern browser for dashboard.

### 2.5 Design and implementation constraints
- Analytics shall be implemented **only** in Hadoop MapReduce (Java, new API).
- Hardware baseline 4 cores / 8 GB RAM; YARN memory limits per CLAUDE.md §5.
- Input is CSV with header; data volume ≈ 14 GB; raw data never stored in git.
- All times UTC.

### 2.6 Assumptions and dependencies
Kaggle data obtainable by manual download or Kaggle CLI; cron available; Hadoop daemons are running when a batch starts; the dataset contains no URLs and no HTTP status codes (hence the definitions in this SRS).

---

## 3. Specific requirements

### 3.1 External interface requirements

#### 3.1.1 Input data interface
CSV, UTF-8, 9 columns (order fixed):

| # | Column | Type | Rule |
|---|---|---|---|
| 1 | event_time | `yyyy-MM-dd HH:mm:ss UTC` | real calendar date/time |
| 2 | event_type | enum | `view`/`cart`/`remove_from_cart`/`purchase` |
| 3 | product_id | long | digits |
| 4 | category_id | long | digits |
| 5 | category_code | string | may be empty |
| 6 | brand | string | may be empty |
| 7 | price | decimal | numeric, ≥ 0 |
| 8 | user_id | long | digits |
| 9 | user_session | string | non-empty |

#### 3.1.2 HDFS interface
Layout per CLAUDE.md §7. Raw partitions are read-only to jobs. Work and report directories are owned by the run and replaced on rerun.

#### 3.1.3 Command-line interfaces

| Command | Parameters |
|---|---|
| `split_by_day.py` | `--input <csv> --out <dir> [--force]` |
| `ingest_to_hdfs.sh` | `<local partitions dir> [--force]` |
| `nightly_batch.sh` | `[--date YYYY-MM-DD] [--backfill --from D --to D] [--queue Q] [--install-cron] [--simulate]` |
| `PipelineDriver` | `--run-id --input --queue --topn 10 --reducers-traffic 4 --reducers-session 8 --work-dir --report-dir` |
| `export_reports.py` | `--run-id [--out reports/]` |
| `verify_with_pandas.py` | `--csv <sample> --run-id <id>` |

#### 3.1.4 User interface — dashboard
Streamlit multi-page app with sidebar run selector. Pages defined in FR-DSH-*. Wireframes in PRD §8.

#### 3.1.5 Output file interfaces
All exported CSVs: UTF-8, header row, `,` separator, `\n` line endings, sorted deterministically, no quoting unless needed. Schemas in §3.5.

---

### 3.2 System features overview
Ingestion (ING) · MapReduce analytics (MR) · Scheduling (SCH) · Reporting/export (RPT) · Dashboard (DSH) · Operations (OPS) · Optional (OPT).

### 3.3 Functional requirements

#### 3.3.1 Ingestion

| ID | Pri | Requirement |
|---|---|---|
| FR-ING-01 | M | `README` shall document downloading the two Kaggle CSV files (CLI and manual) into `data/raw/`. |
| FR-ING-02 | M | `split_by_day.py` shall stream the CSV and write one file `dt=YYYY-MM-DD/events.csv` per UTC date derived from `event_time`, preserving the header in each file. Memory use shall be independent of input size. |
| FR-ING-03 | M | Rows whose date cannot be derived shall be written unchanged to `dt=unknown/events.csv`; no row shall be dropped (rows_in = rows_out + header). |
| FR-ING-04 | M | `ingest_to_hdfs.sh` shall upload partitions to `/clickstream/raw/` and skip existing partitions unless `--force`. |
| FR-ING-05 | M | `make_sample.sh` shall create `data/sample/sample_1m.csv` (header + first 1,000,000 rows) and a tiny `mini_clickstream.csv` fixture exists in test resources. |
| FR-ING-06 | S | Ingestion shall print a summary: partitions written, rows per partition, total bytes. |

#### 3.3.2 MapReduce — shared behaviour

| ID | Pri | Requirement |
|---|---|---|
| FR-MR-01 | M | `ClickEventParser.parse(Text, ClickEvent)` shall return a status (`OK`, `HEADER`, or a specific malformed reason), never throw, and be the only CSV parsing code in the project. |
| FR-MR-02 | M | Every mapper shall increment counter group `ClickstreamQuality`: `TOTAL_RECORDS`, `VALID_RECORDS`, `HEADER_SKIPPED`, `MALFORMED_COLUMN_COUNT`, `MALFORMED_TIMESTAMP`, `MALFORMED_EVENT_TYPE`, `MALFORMED_ID`, `MALFORMED_PRICE`, `MALFORMED_SESSION`, `MISSING_CATEGORY`, `MISSING_BRAND`. |
| FR-MR-03 | M | Jobs shall be submitted to the queue given by `mapreduce.job.queuename` (default `batch`). |
| FR-MR-04 | M | J1, J3 and J5 shall use combiners; combiner correctness shall be unit tested (result with and without combiner is identical). |
| FR-MR-05 | M | Output shall be deterministic (sorted keys; fixed tie-breaks). |
| FR-MR-06 | M | No reducer shall store all values of a key. |
| FR-MR-07 | M | `PipelineDriver` shall verify the invariant `Σ(J1.total + J1.malformed) = TOTAL_RECORDS − HEADER_SKIPPED` and fail the run if violated. |
| FR-MR-08 | S | Job configuration shall be overridable via `-D clickstream.*` keys (`topn`, `topn.metric`, `reducers.*`). |

#### 3.3.3 Parser rules (normative)
Evaluate in this order; first failure decides the counter.

| Order | Condition | Result |
|---|---|---|
| 1 | Line starts with `event_time,` | `HEADER` → `HEADER_SKIPPED` |
| 2 | Split on `,` (keep empties) ≠ 9 fields (incl. blank line) | `MALFORMED_COLUMN_COUNT` |
| 3 | `event_time` length ≥ 19, matches `dddd-dd-dd dd:dd:dd`, month 1–12, day valid for month, hour 0–23, min/sec 0–59 | else `MALFORMED_TIMESTAMP` |
| 4 | `event_type` ∈ {view, cart, remove_from_cart, purchase} | else `MALFORMED_EVENT_TYPE` |
| 5 | `product_id`, `category_id`, `user_id` parse as `long` | else `MALFORMED_ID` |
| 6 | `price` parses as decimal and ≥ 0 | else `MALFORMED_PRICE` |
| 7 | `user_session` non-empty | else `MALFORMED_SESSION` |
| – | `category_code` empty → `MISSING_CATEGORY`; `brand` empty → `MISSING_BRAND` (record stays valid) | |

If rule 3 passed before a later rule failed, the record is **attributable** to its `date HH` bucket; otherwise it is **unattributable**.

#### 3.3.4 Job J1 — HourlyTraffic (Report 1 & Report 3A)

**FR-MR-10 (Must)**

- **Input:** raw CSV (any `dt=*` glob).
- **Mapper:** parse. Valid → key `Text("yyyy-MM-dd HH")`, value `EventCountWritable` with exactly one of `views|carts|removes|purchases` = 1. Malformed & attributable → same key kind, `malformed = 1`. Malformed & unattributable → key `"INVALID_TIME"`, `malformed = 1`.
- **Combiner = Reducer logic:** field-wise sum.
- **Reducer output** (TextOutputFormat, tab-separated):
  `date \t hour \t total \t views \t carts \t removes \t purchases \t malformed \t error_rate`
  where `total = views+carts+removes+purchases` and `error_rate = malformed / (total + malformed)` printed with 6 decimals (`0.000000` when denominator 0). The `INVALID_TIME` key is written as `NA\tNA\t0\t0\t0\t0\t0\t<n>\t1.000000`.
- **Reducers:** 4 (configurable). **Partitioner:** default hash.

#### 3.3.5 Job J2 — PeakHour (Report 1)

**FR-MR-11 (Must)**

- **Input:** J1 output. **Mapper** skips `NA` rows; emits `("D|"+date) → "hour:total"` and `("H|"+hour) → "date:total"`.
- **Reducer** (1 reducer; `MultipleOutputs`):
  - key `D|date` → named output `peakday`: `date \t peak_hour \t peak_total \t day_total` (max `total`, tie → smallest hour; `day_total` = Σ totals).
  - key `H|hour` → named output `hourofday`: `hour \t total_events \t days_observed` (`days_observed` = number of dates for which that hour had a J1 row).
- Average per day (exporter) = `total_events / days_observed`.

#### 3.3.6 Job J3 — ViewCount (Report 2)

**FR-MR-12 (Must)**

- **Input:** raw CSV. **Mapper:** only valid `view` events. Emits `"P|"+product_id → ViewCountWritable(1, category_code, brand)`; and, if `category_code` non-empty, `"C|"+category_code → ViewCountWritable(1, "", "")`.
- **Combiner/Reducer:** sum count; for category/brand keep the lexicographically smallest non-empty value seen (deterministic).
- **Reducer** with `MultipleOutputs`: `products/` → `product_id \t category_code \t brand \t views`; `categories/` → `category_code \t views`.
- Uncategorised view total is obtained by counter `MISSING_CATEGORY` restricted to views (custom counter `VIEWS_WITHOUT_CATEGORY`).

#### 3.3.7 Job J4 — TopN (Report 2)

**FR-MR-13 (Must)**

- **Input:** J3 `products/` (run 1) or `categories/` (run 2). N from `clickstream.topn` (default 10).
- **Mapper:** keeps a bounded `TreeMap` of the N best by (`views` desc, `key` asc); in `cleanup()` emits survivors under `NullWritable`.
- **Reducer (1):** merges all candidates with the same ordering; writes `rank \t <original columns>` for ranks 1..N.
- Output dirs: `top_products/`, `top_categories/`.

#### 3.3.8 Job J5 — SessionFunnel (Report 3B)

**FR-MR-14 (Must)**

- **Input:** raw CSV. **Mapper:** valid records → key `user_session`, value `SessionAggWritable{minEpoch (UTC seconds), flags (bit0 view, bit1 cart, bit2 purchase), removeCount}`.
- **Combiner:** `minEpoch = min`, `flags = OR`, `removeCount = Σ`.
- **Reducer:** same merge, streaming. Session date = UTC date of `minEpoch`. Folds into an in-memory `Map<date, long[5]>` = `[sessions, cart_sessions, purchase_sessions, abandoned_sessions, remove_events_in_cart_sessions]`; in `cleanup()` emits one line per date: `date \t sessions \t cart \t purchase \t abandoned \t removes`.
- **Reducers:** 8 (configurable). Memory bound: ≤ number of distinct dates.

#### 3.3.9 Job J6 — DailyFunnel (Report 3B)

**FR-MR-15 (Must)**

- **Input:** J5 output. Mapper passes `date → long[5]`; combiner/reducer (1) sums element-wise. Output: `date \t sessions \t cart_sessions \t purchase_sessions \t abandoned_sessions \t remove_events`.

#### 3.3.10 Scheduling

| ID | Pri | Requirement |
|---|---|---|
| FR-SCH-01 | M | `nightly_batch.sh --install-cron` shall install `0 2 * * *` invoking the script with default date = yesterday UTC. |
| FR-SCH-02 | M | The script shall acquire an exclusive `flock` on `/tmp/clickstream_batch.lock`; if held, exit with code 75 and log "already running". |
| FR-SCH-03 | M | `--date D` shall run `run_id=daily-D` on `/clickstream/raw/dt=D`; `--backfill --from --to` shall run `backfill-from_to` on the matching partitions. |
| FR-SCH-04 | M | Before running, the script shall check HDFS and YARN availability (`hdfs dfsadmin -safemode get`, `yarn node -list`) and the existence of the input partition; otherwise exit with code 2/3/4 respectively and write `FAILED` run_meta. |
| FR-SCH-05 | M | Re-running a `run_id` shall delete and recreate its work and report directories (idempotent). Final reports shall be written to a temp dir then renamed so partial results are never visible. |
| FR-SCH-06 | M | Jobs shall run on queue `batch` configured in `capacity-scheduler.xml` (`default` 30 %, `batch` 70 %, max capacity 100 %). |
| FR-SCH-07 | M | `PipelineDriver` shall write `run_meta.json` (see §3.5.6) with status, per-job YARN application id, start/end, duration, key counters. |
| FR-SCH-08 | S | A failed job shall be retried once at pipeline level (in addition to YARN task retries `mapreduce.map.maxattempts=4`); second failure aborts the run with exit code 1. |
| FR-SCH-09 | S | All output shall go to `logs/nightly-<run_id>-<timestamp>.log`; logs older than 30 days are pruned. |
| FR-SCH-10 | S | `--simulate` / `simulate_nightly.sh` shall advance a pointer file through dataset days to emulate consecutive nights. |

#### 3.3.11 Reporting / export

| ID | Pri | Requirement |
|---|---|---|
| FR-RPT-01 | M | `export_reports.py` shall read HDFS outputs (`hdfs dfs -cat`) in streaming fashion and write the CSV files in §3.5 to `reports/<run_id>/` with headers. |
| FR-RPT-02 | M | It shall compute derived columns (rates, averages, shares) with the formulas in §3.5; division by zero yields empty cell, not an error. |
| FR-RPT-03 | M | It shall write `report_meta.json` with the definitions text, input row counts, counters, and generation timestamp. |
| FR-RPT-04 | M | It shall copy `run_meta.json` into the report folder. |
| FR-RPT-05 | M | Rerun of export for the same `run_id` shall produce byte-identical CSVs. |
| FR-RPT-06 | S | `verify_with_pandas.py` shall recompute all report metrics independently from the CSV sample and compare with the exported CSVs, printing per-check PASS/FAIL and `ALL CHECKS PASSED`. |

#### 3.3.12 Dashboard

| ID | Pri | Requirement |
|---|---|---|
| FR-DSH-01 | M | Sidebar lists available runs (newest first) from `reports/`; selecting one reloads all pages. |
| FR-DSH-02 | M | **Overview**: KPI cards (total events, days covered, overall peak hour, malformed %, cart abandonment %, conversion %, run status & duration) + hourly traffic line chart. |
| FR-DSH-03 | M | **Peak Traffic**: HOD bar chart with the peak hour highlighted; stacked event-type chart per hour; table of per-day peak hours (shown when run spans > 1 day). |
| FR-DSH-04 | M | **Top Pages**: Top-10 products bar chart + table (rank, product_id, brand, category, views, % of all views); Top-10 categories chart + table; KPI "uncategorised view share". |
| FR-DSH-05 | M | **Errors & Funnel**: Section A (data-quality) hourly/daily malformed-rate chart, breakdown by reason; Section B (behavioural) abandonment, remove rate, conversion charts, funnel (view → cart → purchase events). Each metric displays its formula caption. |
| FR-DSH-06 | M | **Job Runs**: table of all runs with status, timings, YARN app ids, counters; failed runs highlighted. |
| FR-DSH-07 | S | Every table has a "Download CSV" button. |
| FR-DSH-08 | S | If `reports/` is empty, show a helpful empty-state message with the command to generate data. |
| FR-DSH-09 | C | Compare two runs side by side. |

#### 3.3.13 Optional

| ID | Pri | Requirement |
|---|---|---|
| FR-OPT-01 | C | `inject_http_status.py` may add a **synthetic** `http_status` column (seeded RNG; e.g. 97 % 200, 1.5 % 404, 1 % 500, 0.5 % 503) producing a separate dataset and separate report labelled "SYNTHETIC". It shall never feed the real error-rate metrics. |
| FR-OPT-02 | C | Top-N by purchases via `clickstream.topn.metric=purchase`. |
| FR-OPT-03 | C | Top-level category roll-up. |

---

### 3.4 Data requirements

#### 3.4.1 Custom Writables

| Class | Fields | Used by |
|---|---|---|
| `EventCountWritable` | `long views, carts, removes, purchases, malformed` | J1 |
| `ViewCountWritable` | `long count; String category; String brand` | J3 |
| `SessionAggWritable` | `long minEpoch; byte flags; int removeCount` | J5 |

All implement `Writable`, have no-arg constructors, `merge(other)` methods used by combiners/reducers, and round-trip unit tests.

#### 3.4.2 Worked example (illustrative synthetic rows)

Input (fixture excerpt):
```
event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session
2019-11-01 00:00:02 UTC,view,1001,2001,electronics.smartphone,acme,199.90,11,sess-A
2019-11-01 00:00:40 UTC,cart,1001,2001,electronics.smartphone,acme,199.90,11,sess-A
2019-11-01 00:59:59 UTC,view,1002,2002,,,5.00,12,sess-B
2019-11-01 01:00:00 UTC,view,1001,2001,electronics.smartphone,acme,199.90,13,sess-C
2019-11-01 01:10:00 UTC,purchase,1001,2001,electronics.smartphone,acme,199.90,11,sess-A
2019-13-01 01:10:00 UTC,view,1001,2001,electronics.smartphone,acme,199.90,14,sess-D
2019-11-01 01:20:00 UTC,view,abc,2001,x.y,acme,3.00,15,sess-E
2019-11-01 01:30:00 UTC,view,1001,2001
```
Expected:
- **J1:** `2019-11-01 00` → total 3 (views 2, carts 1), malformed 0; `2019-11-01 01` → total 2 (views 1, purchases 1), malformed 1 → error_rate 1/3 = 0.333333 (the `abc` row passed the timestamp rule, so it is attributable to hour 01). `INVALID_TIME` → malformed 2 (the month-13 row and the 4-column row, which fails column-count before any timestamp is read).
- **Counters:** TOTAL_RECORDS 8 (header excluded) · VALID 5 · MALFORMED_TIMESTAMP 1 · MALFORMED_ID 1 · MALFORMED_COLUMN_COUNT 1 · MISSING_CATEGORY 1 · MISSING_BRAND 1.
- **Invariant:** J1 Σ(total+malformed) = 5 + 3 = 8 = TOTAL_RECORDS ✔.
- **J2:** peak hour of 2019-11-01 → hour 0 (3 vs 2), day_total 5.
- **J3/J4:** product 1001 → views 2 (rank 1), product 1002 → views 1 (rank 2); categories: `electronics.smartphone` → 2; one view without category.
- **J5/J6 (2019-11-01):** sessions = 3 valid sessions (A, B, C); cart_sessions 1 (A); purchase_sessions 1 (A); abandoned 0 → abandonment 0 %; remove events 0.

*(The fixture shipped in the repository must reproduce exactly these numbers; adjust the narrative if the fixture grows.)*

#### 3.4.3 Data retention
Raw partitions kept indefinitely; work directories of runs older than 14 days may be pruned by `nightly_batch.sh --prune`; reports kept.

---

### 3.5 Output (report) schemas

All dates `YYYY-MM-DD`, hour integer 0–23, rates as percentages with 4 decimals unless noted.

| File | Columns | Derivation |
|---|---|---|
| `traffic_hourly.csv` | date, hour, total, views, carts, removes, purchases, malformed, malformed_rate_pct | J1 (`malformed_rate_pct = error_rate×100`) |
| `peak_hour_daily.csv` | date, peak_hour, peak_total, day_total, peak_share_pct | J2 `peakday`; `peak_total/day_total×100` |
| `hour_of_day_profile.csv` | hour, total_events, days_observed, avg_events_per_day | J2 `hourofday`; `total/days_observed` |
| `top_products.csv` | rank, product_id, category_code, brand, views, share_of_all_views_pct | J4; denominator = Σ views of all products (from J3 counter `VIEW_EVENTS`) |
| `top_categories.csv` | rank, category_code, views, share_of_categorised_views_pct | J4 |
| `quality_daily.csv` | date, total_records, valid, malformed, malformed_rate_pct | aggregate of J1 per date |
| `quality_reasons.csv` | reason, count | from counters (whole run) |
| `funnel_daily.csv` | date, sessions, cart_sessions, purchase_sessions, abandoned_sessions, cart_abandonment_rate_pct, conversion_rate_pct, cart_events, remove_events, remove_rate_pct | J6 + J1 per date. `abandonment = abandoned/cart_sessions`; `conversion = purchase_sessions/sessions`; `remove_rate = remove_events/cart_events` |
| `report_meta.json` | definitions, counters, row counts, generated_at, hadoop_version | exporter |
| `run_meta.json` | see §3.5.6 | driver |

##### 3.5.6 run_meta.json
```json
{
  "run_id": "daily-2019-11-01",
  "status": "SUCCESS",
  "start": "2026-10-10T02:00:03Z",
  "end": "2026-10-10T02:06:44Z",
  "duration_sec": 401,
  "input": "/clickstream/raw/dt=2019-11-01",
  "queue": "batch",
  "jobs": [
    {"name": "J1_HourlyTraffic", "app_id": "application_…", "state": "SUCCEEDED",
     "duration_sec": 61, "maps": 2, "reduces": 4,
     "counters": {"TOTAL_RECORDS": 0, "MALFORMED_TIMESTAMP": 0, "SHUFFLE_BYTES": 0}}
  ],
  "invariant_ok": true
}
```
(Values illustrative; structure normative. Failed runs set `status:"FAILED"`, `failed_job`, `error`.)

---

## 4. Non-functional requirements

| ID | Category | Requirement |
|---|---|---|
| NFR-01 | Correctness | `verify_with_pandas.py` reports zero mismatches on the 1M-row sample for every report file. |
| NFR-02 | Correctness | Reconciliation invariant (FR-MR-07) holds on every run. |
| NFR-03 | Performance (target) | One daily partition (≤ 400 MB) end-to-end < 10 min; full backfill < 90 min on 4 cores / 8 GB. Measured values shall be recorded; missing a target requires analysis in `docs/benchmarks.md`, not code hacks. |
| NFR-04 | Efficiency | Combiners shall reduce J1 and J3 shuffle bytes by ≥ 90 % versus no combiner (documented via `REDUCE_SHUFFLE_BYTES` / `MAP_OUTPUT_BYTES`). |
| NFR-05 | Reliability | Idempotent reruns; no partial reports published; task retries enabled; pipeline retry once. |
| NFR-06 | Scalability | No design element depends on a single node's memory; increasing reducers or nodes shall not require code changes; hot-key skew mitigated by combiners. |
| NFR-07 | Observability | Counters, per-job YARN app ids, durations and logs available for every run; dashboard Job Runs page. |
| NFR-08 | Maintainability | Parser is the single validation authority; Google Java Format; unit-test coverage of parser/writables/mapper/reducer logic ≥ 80 % line coverage (JaCoCo). |
| NFR-09 | Portability | Setup reproducible on Ubuntu 22.04/WSL2; Docker Compose alternative; versions pinned. |
| NFR-10 | Usability | Dashboard loads a run in < 3 s; every metric has a formula caption; colour-blind-safe palette. |
| NFR-11 | Security | No credentials in repo; Kaggle token via env/`~/.kaggle`; Hadoop UIs bound to localhost; dataset licence respected. |
| NFR-12 | Documentation | README, architecture diagram, benchmarks, demo script, final report delivered. |

---

## 5. Configuration parameters

| Key | Default | Meaning |
|---|---|---|
| `mapreduce.job.queuename` | `batch` | YARN queue |
| `clickstream.topn` | `10` | N for Top-N |
| `clickstream.topn.metric` | `view` | `view` or `purchase` (optional) |
| `clickstream.reducers.traffic` | `4` | J1 reducers |
| `clickstream.reducers.views` | `4` | J3 reducers |
| `clickstream.reducers.session` | `8` | J5 reducers |
| `mapreduce.map.memory.mb` / `reduce.memory.mb` | `1024` / `2048` | container sizes |
| `dfs.blocksize` | `134217728` | 128 MB |
| `dfs.replication` | `1` (pseudo) / `3` (cluster) | |

---

## 6. Verification & traceability

| Requirement(s) | Verification method | Artifact |
|---|---|---|
| FR-ING-02/03 | Unit test + row-count check | `test_split_by_day.py` |
| FR-MR-01, §3.3.3 | JUnit parser tests (one per rule + boundaries) | `ClickEventParserTest` |
| FR-MR-04, NFR-04 | Combiner on/off test + counter comparison | `CombinerEquivalenceTest`, benchmarks |
| FR-MR-10…15 | LocalJobRunner integration on fixture vs golden TSV | `PipelineIT`, `expected/*.tsv` |
| FR-MR-07 | Driver invariant test with injected corruption | `InvariantTest` |
| FR-SCH-02 | Start two runs concurrently; second exits 75 | demo script |
| FR-SCH-05, FR-RPT-05 | Run twice; `diff -r` of reports empty | `scripts/check_idempotent.sh` |
| FR-SCH-06 | RM UI shows jobs on `batch` queue | screenshot in benchmarks |
| FR-RPT-06, NFR-01 | pandas golden check | `verify_with_pandas.py` |
| FR-DSH-01…08 | Manual checklist with screenshots | `docs/demo-script.md` |
| NFR-03 | Timed runs, hardware recorded | `docs/benchmarks.md` |

### 6.1 Acceptance criteria (project)
1. `mvn verify` passes, JaCoCo ≥ 80 % on logic packages.
2. Golden check prints `ALL CHECKS PASSED` on the sample.
3. Cron-triggered run on a fresh day partition publishes reports and the dashboard shows them with no manual steps.
4. Injected corrupt lines raise the malformed rate in the dashboard by the expected amount and the invariant still passes.
5. Two consecutive runs of the same `run_id` yield identical reports.
6. A full backfill completes; benchmark table (per-job time, shuffle bytes, counters) is filled in.

---

## Appendix A — Capacity Scheduler snippet (normative intent)
```xml
<property><name>yarn.scheduler.capacity.root.queues</name><value>default,batch</value></property>
<property><name>yarn.scheduler.capacity.root.default.capacity</name><value>30</value></property>
<property><name>yarn.scheduler.capacity.root.batch.capacity</name><value>70</value></property>
<property><name>yarn.scheduler.capacity.root.batch.maximum-capacity</name><value>100</value></property>
<property><name>yarn.scheduler.capacity.root.batch.user-limit-factor</name><value>1</value></property>
```
(`yarn-site.xml`: `yarn.resourcemanager.scheduler.class=org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler`.)

## Appendix B — Exit codes of `nightly_batch.sh`
`0` success · `1` job failure · `2` HDFS unavailable · `3` YARN unavailable · `4` input partition missing · `64` bad arguments · `75` another run holds the lock.

## Appendix C — Glossary of counters added beyond the parser
`VIEW_EVENTS` (valid view events, J3), `VIEWS_WITHOUT_CATEGORY` (J3), `SESSIONS_SEEN` (J5 reducer input groups), `DATES_EMITTED` (J5/J6).
