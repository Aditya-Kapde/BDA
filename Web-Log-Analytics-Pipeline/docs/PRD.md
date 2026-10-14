# PRD — Batch MapReduce Clickstream Log Analysis

| | |
|---|---|
| **Project** | #53 Batch MapReduce Clickstream Log Analysis |
| **Type** | College-level Big Data project |
| **Document** | Product Requirements Document (what & why). Exact technical requirements live in `SRS.md`; developer/AI working rules in `CLAUDE.md`. |
| **Status** | Draft v1.0 |

---

## 1. Background and problem

E-commerce platforms accumulate massive clickstream logs — every product view, add-to-cart, removal and purchase is an event. At this scale (tens to hundreds of millions of rows, many GB per month) the logs are too large for ad-hoc querying on a single machine or a spreadsheet/SQL tool, so extracting behavioural insight (when traffic peaks, which pages dominate, how often things go wrong) is **delayed by days**.

**Problem statement:** Business and engineering teams cannot get timely, reliable answers about traffic peaks, popular pages and failure rates because the raw logs cannot be queried interactively.

**Proposed solution:** Store the raw logs on **HDFS**, run a **MapReduce pipeline** on **YARN** every night, and publish the results as three standard reports on a **batch report dashboard**, so yesterday's numbers are ready the next morning.

## 2. Goals and non-goals

### Goals
| ID | Goal | Measure of success |
|---|---|---|
| G1 | Store raw clickstream on HDFS in a day-partitioned layout | 100 % of dataset ingested; `dt=` partitions verifiable with `hdfs dfs -ls` |
| G2 | Produce **peak-traffic-hour**, **top-10-page**, **error-rate** reports with MapReduce | Reports match an independent pandas computation on a 1M-row sample (zero mismatches) |
| G3 | Run the pipeline **nightly, unattended** under YARN scheduling | A cron-triggered run completes and publishes reports with no manual steps |
| G4 | Make results consumable | Dashboard shows all reports for any processed run in ≤ 3 clicks |
| G5 | Demonstrate big-data engineering skill | Combiners, counters, bounded memory, idempotent reruns, documented benchmarks |

### Non-goals
- Real-time or streaming analytics (no Kafka/Spark Streaming).
- Using Spark, Hive, Pig, Impala or any engine other than Hadoop MapReduce for the analytics.
- Machine learning, recommendation, user identity resolution, fraud detection.
- Production-grade security, multi-tenant access control, or HA Hadoop.
- Time-zone conversion (everything is UTC).

## 3. Users and personas

| Persona | Needs | How the product serves them |
|---|---|---|
| **Business Analyst "Asha"** | Know when to schedule campaigns and which products/categories get attention; see how much shopping intent is lost | Peak-hour charts, Top-10 products/categories, cart-abandonment trend |
| **Data/Platform Engineer "Ravi"** | Trust the pipeline: runs on time, data-quality problems are visible, reruns are safe | Nightly schedule, run history with YARN IDs and durations, malformed-record rate, idempotent reruns |
| **Course Evaluator "Prof. Mehta"** | Verify correct use of HDFS, MapReduce, YARN and a working end-to-end system | Architecture doc, benchmarks, demo script, reproducible setup |

*(Personas are fictional and used only to shape requirements.)*

## 4. Dataset

Kaggle **eCommerce behavior data from multi-category store** (REES46): `2019-Oct.csv`, `2019-Nov.csv`. One row = one user event with columns `event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session`. Event types: `view`, `cart`, `remove_from_cart`, `purchase`. Approximately 14 GB / 100M+ rows combined (confirm after download).

**Key data limitations (shape the product):**
1. There are **no URLs** → a "page" is a **product page** (`product_id`); categories are a secondary ranking.
2. There are **no HTTP status codes** → "error rate" is defined as **data-quality error rate** (malformed records) plus **behavioural failure** (cart abandonment, remove-from-cart rate). See §6.3.
3. `category_code` and `brand` are frequently blank → reported as missing, not as errors.
4. Data covers only Oct–Nov 2019 → "nightly" is **simulated** by replaying day partitions; the scheduling mechanism itself is real.

## 5. Scope (MoSCoW)

**Must have**
- Day-partitioned HDFS ingestion of the dataset.
- MapReduce jobs J1–J6 producing the three reports (SRS §3.4).
- YARN execution on a dedicated Capacity Scheduler queue `batch`.
- Nightly scheduling via cron with lock, logging, exit codes, idempotent reruns.
- Export of results to CSV and a dashboard with five views (Overview, Peak Traffic, Top Pages, Errors & Funnel, Job Runs).
- Automated tests incl. golden comparison against pandas.
- Benchmarks and final documentation.

**Should have**
- Backfill mode over a date range.
- Reconciliation check (records in = records out + malformed).
- `simulate_nightly.sh` to demo several consecutive nights quickly.
- Optional multi-node docker-compose cluster.

**Could have**
- Top-level category roll-up (first token of `category_code`).
- Top-10 by purchases as an alternative metric (`clickstream.topn.metric=purchase`).
- FR-OPT-01: synthetic HTTP-status injection (labelled synthetic; never mixed with real metrics).
- Comparison chart "MapReduce vs single-machine pandas" runtime on growing input sizes.

**Won't have (this release)** — streaming, Spark/Hive, authentication, alerting emails.

## 6. Product requirements (functional overview)

### 6.1 Report 1 — Peak-traffic-hour
**User story:** *As Asha, I want to see which hour of the day has the most traffic, so I can time campaigns.*

Delivers:
- **Hourly traffic table**: for every `date × hour` — total, views, carts, removes, purchases.
- **Peak hour per day**: hour (0–23 UTC) with maximum traffic, its volume, and its share of the day's total.
- **Hour-of-day profile**: total and average-per-day events for each hour 0–23 (the chart "when do shoppers show up").

Acceptance: for a given sample, peak hour of every day equals pandas result; ties resolve to the earliest hour.

### 6.2 Report 2 — Top-10 pages
**User story:** *As Asha, I want the 10 most viewed product pages (and categories), so I can feature them.*

Delivers: `rank, product_id, category_code, brand, views` for 10 products; `rank, category_code, views` for 10 categories (blank category excluded from ranking but its share reported). Ties by `product_id` ascending / category name ascending.

Acceptance: counts equal pandas `value_counts` on views of the sample; exactly 10 rows (or fewer if fewer exist).

### 6.3 Report 3 — Error rate
**User story:** *As Ravi, I want to know how clean the incoming logs are and how often shoppers fail to complete purchases, so I can raise data problems and measure friction.*

Two clearly separated sections:

**A. Data-quality error rate** (per hour and per day)
`malformed ÷ (valid + malformed)`, with breakdown by reason (column count, timestamp, event type, id, price, session). Missing category/brand rates shown as informational.

**B. Behavioural failure rate** (per day)
- **Cart abandonment rate** = sessions with `cart` but no `purchase` ÷ sessions with `cart`.
- **Remove rate** = `remove_from_cart` events ÷ `cart` events.
- Supporting: sessions, purchase-conversion = purchase sessions ÷ sessions.

Acceptance: both sections match pandas on the sample; the dashboard labels the two sections distinctly and states each formula in a tooltip/caption.

### 6.4 Nightly batch scheduling
**User story:** *As Ravi, I want the pipeline to run itself every night and be safe to rerun.*

- Cron triggers at 02:00 server time; target business date = yesterday (UTC) unless overridden.
- A lock prevents overlapping runs.
- Rerunning the same date replaces its outputs (idempotent).
- Failure → non-zero exit, error log, run marked `FAILED` in `run_meta.json`, dashboard shows it.
- Jobs are submitted to YARN queue `batch` so ad-hoc work on `default` is not starved.

### 6.5 Batch report dashboard
Five views (wireframes in §8): **Overview**, **Peak Traffic**, **Top Pages**, **Errors & Funnel**, **Job Runs**. Run selector in the sidebar; tables downloadable as CSV. The dashboard reads exported files only.

## 7. Non-functional expectations (summary)

| Area | Expectation (details in SRS §4) |
|---|---|
| Correctness | Zero mismatches vs pandas golden check; reconciliation invariant holds |
| Performance (targets, to be measured) | One daily partition end-to-end < 10 min; full 2-month backfill < 90 min on an 8 GB / 4-core laptop pseudo-cluster. If missed, record real numbers and analysis — not a failure of the project |
| Reliability | Idempotent reruns; failed run never leaves partial reports visible |
| Scalability | Design (combiners, bounded memory, no single hot reducer) scales to more nodes/days without code change |
| Usability | Dashboard loads a run in < 3 s; every metric has a definition caption |
| Reproducibility | README + scripts rebuild the environment; pinned versions |

## 8. Dashboard wireframes (text)

**Overview**
```
┌ Run: [daily-2019-11-01 ▼]        Status: ✔ SUCCESS   Duration: 06m 41s ┐
│ [Total events 312,408] [Peak hour 17:00 UTC] [Malformed 0.02%]         │
│ [Cart abandonment 71.4%] [Conversion 2.9%]                             │
│ Hourly traffic line chart (24 points)                                  │
└────────────────────────────────────────────────────────────────────────┘
```
**Peak Traffic** — bar chart hour-of-day (0–23) with peak highlighted; table "peak hour per day" (backfill runs); stacked bars of event types per hour.
**Top Pages** — horizontal bar Top-10 products (label = `product_id · brand · category`), Top-10 categories, share-of-views-covered KPI.
**Errors & Funnel** — Section A: malformed-rate line per day + reason breakdown bar; Section B: abandonment & remove-rate trend, funnel (views → carts → purchases).
**Job Runs** — table of runs: run_id, status, start/end, duration, YARN app IDs per job, input bytes, key counters (TOTAL_RECORDS, MALFORMED_*), reducers used.

## 9. Success metrics

| Metric | Target |
|---|---|
| Report accuracy vs pandas (sample) | 100 % match |
| Nightly run success on 7 consecutive simulated nights | 7/7 |
| Idempotency (rerun gives identical CSVs) | byte-identical |
| Daily partition runtime | < 10 min (measured and documented) |
| Dashboard views implemented | 5/5 |
| Documentation completeness | README, architecture diagram, benchmarks, demo script, final report |

## 10. Assumptions, dependencies, risks

| # | Risk / assumption | Impact | Mitigation |
|---|---|---|---|
| R1 | Laptop RAM/CPU too small for full dataset | Slow runs, container kills | Tune YARN memory (CLAUDE.md §5); develop on sample; process day partitions; document hardware |
| R2 | Dataset has no errors/URLs | Report ambiguity | Fixed definitions (§4, §6.3), confirm with instructor, optional synthetic stretch clearly labelled |
| R3 | Data skew: hot products, giant bot sessions | Slow reducers | Combiners; streaming reducers; 4–8 reducers; no value buffering |
| R4 | Header/malformed lines crash jobs | Run failure | Parser never throws; counters instead of exceptions |
| R5 | Hadoop/Java version mismatch | Setup failures | Pin Java 11 + Hadoop 3.3.6; scripted setup |
| R6 | WSL2/Windows networking issues | Blocked demo | Provide docker-compose alternative; rehearse demo |
| R7 | Sessions spanning midnight | Metric ambiguity | Session belongs to date of its first event; documented |
| R8 | Small per-day files (≈ 180–300 MB) underuse cluster | Low parallelism | Acceptable; backfill demonstrates scale; consider `CombineTextInputFormat` only if profiling shows benefit |
| R9 | Time overrun | Missed deadline | Milestones M0–M9 with Must-haves first |
| D1 | Kaggle account/API access | Needed for download | Manual download fallback documented |

## 11. Milestones (suggested 6-week plan)

| Week | Milestones | Output |
|---|---|---|
| 1 | M0 Environment, M1 Data/partitioning | Hadoop running, sample + partitions on HDFS |
| 2 | M2 Parser, M3 J1 Traffic | Peak-hour inputs verified vs pandas |
| 3 | M4 J2 Peak, M5 J3/J4 Top-N | Reports 1 and 2 complete |
| 4 | M6 J5/J6 Funnel, M7 PipelineDriver + scheduling | Report 3 + nightly run |
| 5 | M8 Export + dashboard | Five dashboard views |
| 6 | M9 Backfill, benchmarks, report, demo rehearsal | Final submission |

## 12. Deliverables

1. Source repository (Java, scripts, dashboard, tests, configs).
2. `CLAUDE.md`, `PRD.md`, `SRS.md`, `README.md`.
3. Architecture diagram (`docs/architecture.png`).
4. Benchmark report (`docs/benchmarks.md`): hardware, input sizes, per-job time, counters, YARN settings, combiner on/off comparison.
5. Final project report (`docs/final-report.md`) incl. definitions, results, limitations.
6. Demo (≈ 10 min) following `docs/demo-script.md`: show HDFS layout → trigger nightly run → watch YARN UI → open dashboard → rerun idempotency → show failure handling.
7. Slide deck (optional).

## 13. Demo script outline

1. `hdfs dfs -ls /clickstream/raw` — partitions exist.
2. Open YARN RM UI (`:8088`) — show `batch` queue config.
3. `scripts/nightly_batch.sh --date 2019-11-01` — jobs J1…J6 appear in RM UI.
4. Open dashboard — walk through the three reports.
5. Rerun same date — outputs identical.
6. Corrupt a few lines in a partition → rerun → malformed rate rises and is visible in the dashboard (demonstrates the error-rate report).
7. Show benchmark table and combiner effect (shuffle bytes counters).

## 14. Open questions (defaults in bold)
1. Is the product-page / cart-abandonment interpretation acceptable to the evaluator? **Yes (assume)**.
2. Single-node vs multi-node expectation? **Single-node pseudo-distributed; multi-node optional**.
3. Is Streamlit acceptable as the "batch report dashboard"? **Yes**.
4. Is a nightly run on replayed historical days acceptable for the demo? **Yes**.
