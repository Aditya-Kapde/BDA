# CLAUDE.md — Batch MapReduce Clickstream Log Analysis

> Project #53 · College Big Data project · Read this file fully before writing any code.
> Companion docs: `PRD.md` (what & why) and `SRS.md` (exact requirements, job designs, schemas).
> If this file and `SRS.md` ever disagree, **SRS.md wins**; fix this file afterwards.

---

## 1. Project in one paragraph

An e-commerce platform produces hundreds of millions of clickstream events that are too large for ad-hoc querying, so insights (busiest hours, most-viewed pages, error rates) arrive days late. This project stores raw clickstream CSV logs on **HDFS** and runs a **pure Hadoop MapReduce (Java) pipeline**, scheduled nightly on **YARN**, that produces three reports — **peak-traffic-hour**, **top-10 pages**, and **error-rate** — plus a **batch report dashboard** that visualises them. Dataset: Kaggle *"eCommerce behavior data from multi-category store"* (REES46), months 2019-Oct and 2019-Nov.

**Out of scope on purpose:** Spark, Hive, Pig, Kafka, real-time/streaming. The assignment is about MapReduce + HDFS + YARN. Do not "improve" the project by swapping them in.

---

## 2. Non-negotiable definitions (read twice)

The dataset does **not** contain URLs or HTTP status codes, so the assignment terms must be mapped onto what the data really has. These mappings are fixed project decisions:

| Assignment term | Definition used in this project |
|---|---|
| **Business day / time zone** | The UTC calendar date of `event_time`. All hours are **UTC, 0–23**. Never convert time zones. |
| **Traffic** | Count of **valid events** (all four event types) in a bucket. |
| **Peak-traffic-hour** | (a) per day: the hour 0–23 with the highest traffic (ties → earliest hour); (b) overall hour-of-day profile: total and average-per-day traffic for each hour 0–23. |
| **Page** | A **product page** = one `product_id`, counted only for `event_type = view`. A secondary report ranks **categories** (`category_code`) the same way. |
| **Top-10 pages** | 10 highest `view` counts. Ties broken by `product_id` ascending (deterministic). |
| **Error** (two kinds, always reported separately) | **(1) Data-quality error** = a record that fails the parsing/validation rules in §3.3 (malformed record). **(2) Behavioural failure** = cart abandonment (session has a `cart` event but no `purchase`) and remove-from-cart rate. |
| **Error rate (data quality)** | `malformed / (valid + malformed)` per hour and per day. |
| **Cart abandonment rate** | `abandoned_sessions / sessions_with_cart`, session = `user_session`, assigned to the UTC date of its **first** event. |
| **Remove rate** | `remove_from_cart events / cart events` per day. |
| **Duplicates** | Raw duplicates are **not** removed. Each row is one event. |
| **Nightly batch** | A run that processes one day-partition `dt=YYYY-MM-DD` (default: yesterday) into a `run_id` such as `daily-2019-11-01`. A **backfill** run processes `dt=*` into `backfill-<from>_<to>`. |

Never silently change a definition. If one must change, update `SRS.md`, this table, the tests, and the dashboard labels in the same commit.

An *optional stretch* (SRS FR-OPT-01) injects **synthetic** HTTP status codes. If implemented it must be clearly labelled "synthetic" everywhere and must never be mixed into the real metrics above.

---

## 3. Dataset facts

### 3.1 Source
Kaggle slug: `mkechinov/ecommerce-behavior-data-from-multi-category-store`. Files: `2019-Oct.csv`, `2019-Nov.csv` (CSV, header row, UTF-8, comma-separated). Roughly 14 GB uncompressed and on the order of 100M+ rows in total — **verify actual sizes after download and record them in `docs/benchmarks.md`**. Check the licence/attribution on the Kaggle page and cite REES46 in the final report. Raw data is **never committed to git**.

### 3.2 Schema (9 columns, in this order)

| # | Column | Type | Notes |
|---|---|---|---|
| 1 | `event_time` | string | Format `yyyy-MM-dd HH:mm:ss UTC`, e.g. `2019-11-01 00:00:02 UTC` |
| 2 | `event_type` | enum | `view`, `cart`, `remove_from_cart`, `purchase` |
| 3 | `product_id` | long | |
| 4 | `category_id` | long | |
| 5 | `category_code` | string | Dotted path, e.g. `electronics.smartphone`. **Often empty.** |
| 6 | `brand` | string | **Often empty.** Lower-case. |
| 7 | `price` | decimal | ≥ 0 expected |
| 8 | `user_id` | long | |
| 9 | `user_session` | string | UUID-like; one user has many sessions |

Empty `category_code` / `brand` are **missing values, not errors** (count them in counters `MISSING_CATEGORY`, `MISSING_BRAND`).

### 3.3 Malformed-record rules (single source of truth: `ClickEventParser`)
A line is **malformed** if any applies (first failing rule determines the counter): wrong column count ≠ 9 (`MALFORMED_COLUMN_COUNT`); `event_time` not parseable/not a real date-time (`MALFORMED_TIMESTAMP`); `event_type` not one of the four values (`MALFORMED_EVENT_TYPE`); `product_id`/`category_id`/`user_id` not a long (`MALFORMED_ID`); `price` not numeric or `< 0` (`MALFORMED_PRICE`); `user_session` empty (`MALFORMED_SESSION`). The header line (starts with `event_time,`) is counted as `HEADER_SKIPPED`, never as malformed. Blank lines count as `MALFORMED_COLUMN_COUNT`.

---

## 4. Architecture

```
Kaggle CSV ──> scripts/split_by_day.py ──> HDFS /clickstream/raw/dt=YYYY-MM-DD/events.csv
                                                        │
cron 02:00 ──> scripts/nightly_batch.sh  (flock, logging, exit codes)
                    │
                    └─> hadoop jar PipelineDriver  (YARN queue "batch", Capacity Scheduler)
                          │
   scan 1:  J1 HourlyTraffic+Quality ──> J2 PeakHour
   scan 2:  J3 ViewCounts ──> J4 TopN (products) , J4 TopN (categories)
   scan 3:  J5 SessionFunnel ──> J6 DailyFunnel
                          │
                 HDFS /clickstream/work/<run_id>/...
                          │
          scripts/export_reports.py  ──>  reports/<run_id>/*.csv + report_meta.json + run_meta.json
                          │
                 dashboard/app.py (Streamlit; reads only local reports/, never HDFS)
```

Design principles: **three scans of the raw data** (one per concern), each followed by tiny jobs on aggregated output; **combiners everywhere possible**; **no unbounded in-memory collections** in reducers; dashboard is decoupled from Hadoop via exported files.

---

## 5. Tech stack & versions (pin these)

| Component | Version / choice |
|---|---|
| OS | Ubuntu 22.04 (native, VM, or WSL2). Docker Compose supported as an alternative. |
| Java | **OpenJDK 11** |
| Hadoop | **3.3.6**, pseudo-distributed (1 node) as baseline; optional 3-node docker-compose |
| Build | Maven 3.9, `hadoop-client` 3.3.6 scope `provided`, JUnit 5, Mockito |
| YARN scheduler | **Capacity Scheduler** with queues `default` (30 %) and `batch` (70 %) |
| Scheduling | `cron` + `flock` (Airflow/Oozie are out of scope) |
| Python | 3.10+ (tooling, export, dashboard, verification only — **never** for the core analytics) |
| Dashboard | Streamlit + pandas + Plotly |

Resource baseline for an 8 GB laptop (`conf/yarn-site.xml`, `conf/mapred-site.xml`): `yarn.nodemanager.resource.memory-mb=5120`, `yarn.scheduler.maximum-allocation-mb=2048`, `mapreduce.map.memory.mb=1024`, `mapreduce.reduce.memory.mb=2048`, `mapreduce.map.java.opts=-Xmx819m`, `mapreduce.reduce.java.opts=-Xmx1638m`, `dfs.blocksize=134217728`, `dfs.replication=1`.

---

## 6. Repository layout

```
clickstream-mapreduce/
├── CLAUDE.md  PRD.md  SRS.md  README.md
├── pom.xml
├── conf/                       # core-site, hdfs-site, yarn-site, mapred-site, capacity-scheduler .xml
├── src/main/java/edu/bigdata/clickstream/
│   ├── PipelineDriver.java     # runs J1..J6 in order, writes run_meta.json
│   ├── common/                 # ClickEvent, ClickEventParser, EventType, QualityCounter (enum), TimeUtil, RunMeta
│   ├── writable/               # EventCountWritable, ViewCountWritable, SessionAggWritable
│   ├── traffic/                # HourlyTrafficJob + Mapper/Combiner/Reducer          (J1)
│   ├── peak/                   # PeakHourJob + Mapper/Reducer                        (J2)
│   ├── toppages/               # ViewCountJob (J3), TopNJob (J4) + their classes
│   └── funnel/                 # SessionFunnelJob (J5), DailyFunnelJob (J6) + classes
├── src/test/java/...           # unit + LocalJobRunner integration tests
├── src/test/resources/fixtures/ # mini_clickstream.csv + expected/*.tsv
├── scripts/
│   ├── setup_hadoop.sh         # install/config notes → reproducible
│   ├── download_dataset.md     # Kaggle CLI steps
│   ├── make_sample.sh          # first N rows → data/sample/
│   ├── split_by_day.py         # CSV → dt=YYYY-MM-DD files (streaming)
│   ├── ingest_to_hdfs.sh
│   ├── nightly_batch.sh        # cron entry point
│   ├── simulate_nightly.sh     # replays dataset days as "nightly" runs
│   ├── export_reports.py       # HDFS outputs → reports/<run_id>/
│   └── verify_with_pandas.py   # golden check of MR results on the sample
├── dashboard/                  # app.py, pages/, requirements.txt
├── data/                       # raw/, sample/, partitions/  (git-ignored)
├── reports/                    # exported outputs (git-ignored except a tiny demo set)
├── logs/                       # nightly logs (git-ignored)
└── docs/                       # architecture.png, benchmarks.md, demo-script.md, final-report.md
```

Java package root: `edu.bigdata.clickstream`. Artifact: `target/clickstream-mr-1.0.jar`.

---

## 7. HDFS layout

```
/clickstream/raw/dt=YYYY-MM-DD/events.csv        # one file per UTC day; header kept
/clickstream/raw/dt=unknown/events.csv           # rows whose event_time could not be parsed
/clickstream/work/<run_id>/traffic/              # J1 output
/clickstream/work/<run_id>/peak/                 # J2: named outputs peakday-*, hourofday-*
/clickstream/work/<run_id>/viewcounts/           # J3: products/, categories/
/clickstream/work/<run_id>/top_products/         # J4
/clickstream/work/<run_id>/top_categories/       # J4
/clickstream/work/<run_id>/session_partial/      # J5
/clickstream/work/<run_id>/funnel_daily/         # J6
/clickstream/reports/<run_id>/                   # copy of final report files + run_meta.json
```
`run_id` formats: `daily-YYYY-MM-DD`, `backfill-YYYY-MM-DD_YYYY-MM-DD`. Re-running the same `run_id` **deletes and recreates** its work/report dirs (idempotent).

---

## 8. Pipeline jobs (summary; full spec in SRS §3.4)

| Job | Reads | Map output (key → value) | Reduce output | Combiner | Reducers |
|---|---|---|---|---|---|
| **J1 HourlyTraffic** | raw CSV | `"yyyy-MM-dd HH"` (or `"INVALID_TIME"`) → `EventCountWritable{views,carts,removes,purchases,malformed}` | `date\thour\ttotal\tviews\tcarts\tremoves\tpurchases\tmalformed\terror_rate` | yes (sum) | 4 |
| **J2 PeakHour** | J1 out | `"D|date"`→`hour:total`; `"H|hour"`→`date:total` | named outputs `peakday` (date, peak_hour, peak_total, day_total) and `hourofday` (hour, total_events, days_observed) | no | 1 |
| **J3 ViewCount** | raw CSV (views only) | `"P|product_id"` / `"C|category_code"` → `ViewCountWritable{count, category, brand}` | named outputs `products/`, `categories/` | yes | 4 |
| **J4 TopN** (run twice) | J3 products / categories | in-mapper TreeMap top-N → `NullWritable` → candidate | `rank\tkey\t…\tviews` | no | 1 |
| **J5 SessionFunnel** | raw CSV | `user_session` → `SessionAggWritable{minEpoch, flagsMask, removeCount}` | per-reducer partial per-date counters, emitted in `cleanup()` | yes (min/OR/sum) | 8 |
| **J6 DailyFunnel** | J5 out | `date` → partial counters | `date\tsessions\tcart_sessions\tpurchase_sessions\tabandoned_sessions` | yes | 1 |

---

## 9. Commands

```bash
# Build + unit/integration tests (LocalJobRunner, no cluster needed)
mvn -q clean verify

# Start Hadoop (pseudo-distributed)
start-dfs.sh && start-yarn.sh && mapred --daemon start historyserver

# Prepare data
python3 scripts/split_by_day.py --input data/raw/2019-Nov.csv --out data/partitions
scripts/ingest_to_hdfs.sh data/partitions            # puts dt=* dirs under /clickstream/raw

# Run one nightly batch manually
scripts/nightly_batch.sh --date 2019-11-01

# Run a full backfill
scripts/nightly_batch.sh --backfill --from 2019-10-01 --to 2019-11-30

# Direct driver call (what the script wraps)
hadoop jar target/clickstream-mr-1.0.jar edu.bigdata.clickstream.PipelineDriver \
  --run-id daily-2019-11-01 --input '/clickstream/raw/dt=2019-11-01' \
  --queue batch --reducers-traffic 4 --reducers-session 8

# Export & view
python3 scripts/export_reports.py --run-id daily-2019-11-01
streamlit run dashboard/app.py

# Correctness check against pandas on the sample
python3 scripts/verify_with_pandas.py --csv data/sample/sample_1m.csv --run-id sample
```
Cron entry (installed by `nightly_batch.sh --install-cron`): `0 2 * * * /path/scripts/nightly_batch.sh >> logs/cron.log 2>&1`. Web UIs: HDFS `:9870`, YARN RM `:8088`, JobHistory `:19888`.

---

## 10. Java / MapReduce coding rules

1. Use the **new API** (`org.apache.hadoop.mapreduce`). Drivers implement `Tool` and run via `ToolRunner`.
2. **Reuse Writable objects** in `map()`/`reduce()` (declare as fields; call `set()`); never allocate per record.
3. **All parsing goes through `ClickEventParser`**. No mapper parses CSV on its own. Parser is allocation-light (manual split by `indexOf(',')`, no regex, no `SimpleDateFormat`; fixed-position timestamp parsing with range validation).
4. Mappers increment `QualityCounter` enum counters for every outcome (valid, each malformed reason, header, missing category/brand).
5. Every custom `Writable` implements `write/readFields` symmetrically and has a unit test for round-trip serialisation. Provide `toString()` only for text output.
6. Combiners must be **associative and commutative** and output the same type as the map output. Reducers must tolerate being run zero or more times on combiner output.
7. **Never buffer all values of a key** in a reducer (sessions can contain thousands of events). Fold values into running aggregates.
8. In-memory structures are allowed only when bounded by design: top-N `TreeMap` (N=10), per-date counters (≤ ~100 dates).
9. Output must be **deterministic** (stable tie-breaks, sorted keys); tests compare against golden files.
10. Configuration through `-D` generic options / `Configuration` keys prefixed `clickstream.` (e.g. `clickstream.topn=10`, `clickstream.topn.metric=view`). No hard-coded paths.
11. Use SLF4J logging; no `System.out` in job code.
12. Java style: Google Java Format, 100 cols, no wildcard imports.

## 11. Python rules
Type hints, `argparse`, no global state. `split_by_day.py` and `export_reports.py` must **stream** (never load a 9 GB file into memory). `verify_with_pandas.py` may use pandas because it only runs on the sample. Dashboard code only reads `reports/`; it must not import Hadoop tooling.

---

## 12. Testing strategy

| Level | What | How |
|---|---|---|
| Unit | `ClickEventParser` (every malformed rule, timestamp edge cases, missing category/brand), Writable round-trips, `TimeUtil` | JUnit 5 |
| Component | each Mapper/Reducer/Combiner with a mocked `Context` | Mockito (MRUnit is retired — do not use it) |
| Integration | J1…J6 + `PipelineDriver` end-to-end on `mini_clickstream.csv` | `mapreduce.framework.name=local`, `fs.defaultFS=file:///`; compare to `expected/*.tsv` |
| Golden | MR output vs independent pandas implementation on a 1M-row sample | `verify_with_pandas.py` must print `ALL CHECKS PASSED` |
| Reconciliation | `sum(J1 total + malformed) == TOTAL_RECORDS − HEADER_SKIPPED` | built into `PipelineDriver`; fails the run on mismatch |
| System | nightly script on pseudo-distributed cluster, rerun idempotency, lock behaviour | manual checklist in `docs/demo-script.md` |

**`mini_clickstream.csv` must contain at least:** header; blank line; 8-column and 10-column rows; invalid month (`2019-13-01`); unknown event type; non-numeric `product_id`; negative price; empty `user_session`; events at `23:59:59` and `00:00:00`; a session that crosses midnight; empty brand and category; exact duplicate rows; a two-way tie for 10th place in top-N; sessions with cart-only, cart+purchase, purchase-only, view-only.

---

## 13. Implementation order (do not skip ahead)

1. **M0** Environment: Hadoop 3.3.6 pseudo-distributed running, `hdfs dfs -ls /` works, wordcount example runs on YARN.
2. **M1** Data: download, 1M-row sample, `split_by_day.py`, ingest sample + one day to HDFS.
3. **M2** `ClickEvent`, `ClickEventParser`, counters + full parser unit tests.
4. **M3** J1 + tests → verify against pandas on sample.
5. **M4** J2 + tests.
6. **M5** J3 + J4 + tests.
7. **M6** J5 + J6 + tests.
8. **M7** `PipelineDriver`, YARN queue config, `nightly_batch.sh`, lock, idempotency, `run_meta.json`.
9. **M8** `export_reports.py`, dashboard (all 5 pages).
10. **M9** Full-dataset backfill, benchmarks (`docs/benchmarks.md`), final report, demo rehearsal.

Commit after each milestone with message `M<n>: <summary>`. Each milestone must leave `mvn verify` green.

## 14. Definition of Done (project level)
- All FR-* in `SRS.md` implemented or explicitly marked deferred with reason.
- `mvn verify` green; `verify_with_pandas.py` passes on the sample.
- A full 2-month backfill completes on the cluster; benchmark table filled in.
- A nightly run triggered by cron produces reports + dashboard shows the new run without manual steps.
- Rerunning the same `run_id` gives byte-identical report CSVs.
- README lets a stranger reproduce the setup in under an hour.

## 15. Do / Don't

**Do:** ask before altering a definition in §2; keep jobs small and single-purpose; record counters; log YARN application IDs; document every benchmark with hardware specs.
**Don't:** commit data or `target/`; use Spark/Hive; dedupe raw rows; convert time zones; load whole files in Python; mix synthetic and real metrics; leave `System.out` debugging; hide malformed records by silently skipping them (always count them).

## 16. Assumptions to confirm with the course instructor (defaults in bold)
- "Error rate" interpreted as **data-quality + cart-abandonment** because the dataset has no HTTP codes.
- "Page" interpreted as **product page by `product_id`** (and category as a secondary view).
- Cluster is **single-node pseudo-distributed**; multi-node is optional.
- Dashboard tool is free choice (**Streamlit**).
