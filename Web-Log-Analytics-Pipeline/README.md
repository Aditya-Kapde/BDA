# Batch MapReduce Clickstream Log Analysis (Project #53)

A high-performance batch analytics pipeline that processes large-scale e-commerce clickstream logs (~14 GB / 100M+ events) through a pure **Hadoop MapReduce (Java)** pipeline running on **HDFS** and **YARN** under the **Capacity Scheduler**, orchestrated by `cron` and visualized via an interactive **Streamlit** dashboard.

- **Authoritative Specifications:** [docs/PRD.md](docs/PRD.md) · [docs/SRS.md](docs/SRS.md) · [docs/CLAUDE.md](docs/CLAUDE.md)
- **Status & Progress:** [docs/IMPLEMENTATION_STATUS.md](docs/IMPLEMENTATION_STATUS.md) · [docs/IMPLEMENTATION_PLAN.md](docs/IMPLEMENTATION_PLAN.md)
- **Dataset:** Kaggle [eCommerce behavior data from multi-category store (REES46)](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) (`2019-Oct.csv` and `2019-Nov.csv`, ~14 GB).

---

## 1. Architecture

```
Kaggle CSV ──> scripts/split_by_day.py ──> HDFS /clickstream/raw/dt=YYYY-MM-DD/events.csv
                                                        │
cron 02:00 ──> scripts/nightly_batch.sh  (flock, logging, exit codes)
                    │
                    └─> hadoop jar PipelineDriver  (YARN queue "batch", Capacity Scheduler)
                          │
   scan 1:  J1 HourlyTraffic+Quality ──> J2 PeakHour (MultipleOutputs: peakday, hourofday)
   scan 2:  J3 ViewCounts ──> J4 TopN (products) & TopN (categories)
   scan 3:  J5 SessionFunnel ──> J6 DailyFunnel
                          │
                 HDFS /clickstream/work/<run_id>/...
                          │
          scripts/export_reports.py  ──>  reports/<run_id>/*.csv + report_meta.json + run_meta.json
                          │
                 dashboard/app.py (Streamlit: Overview, Peak, Top, Funnel, Job Runs)
```

### Core Design Principles:
- **Pure MapReduce Java Engine:** All core analytics are implemented in pure Hadoop MapReduce using the new `org.apache.hadoop.mapreduce` API (no Spark, PySpark, or Hive).
- **Single Source of Truth Parser:** `ClickEventParser` rigorously evaluates all records across 7 normative rules without throwing exceptions, tracking data quality via `QualityCounter` Hadoop counters.
- **Three Strategic Scans:** Raw data is scanned exactly three times (traffic/quality, views, session funnel), followed by fast aggregation jobs on intermediate outputs.
- **Combiners Everywhere:** Associative, commutative combiners reduce shuffle volume by ≥ 90%.
- **Bounded Reducer Memory:** Zero in-memory buffering of entire session streams.
- **Decoupled Reporting:** The Streamlit dashboard reads exported CSV files from `reports/<run_id>/` and never queries HDFS directly.

---

## 2. Tech Stack & Ports

| Component | Version / Specification |
|---|---|
| Hadoop | **3.3.6** (HDFS & YARN Capacity Scheduler) |
| Runtime Engine | **Hadoop MapReduce (Java 11, new API)** |
| Resource Scheduler | **Capacity Scheduler** (Queues: `default` = 30%, `batch` = 70%) |
| Build Tooling | **Maven 3.9**, JUnit 5, Mockito, JaCoCo (≥ 80% coverage) |
| Automation | **`cron` + `flock`** (`scripts/nightly_batch.sh`) |
| Ingestion & Verification | **Python 3.10+** (standard library streaming + pandas verification) |
| Dashboard | **Streamlit** (5-page interactive UI) |

### Web UIs & Port Bindings:
- **HDFS NameNode UI:** http://localhost:19870 *(remapped from 9870 to prevent Windows Hyper-V port exclusion)*
- **YARN ResourceManager UI:** http://localhost:8088 *(monitor jobs on the `batch` queue)*
- **JobHistoryServer UI:** http://localhost:19888
- **Streamlit Dashboard:** http://localhost:8501

---

## 3. Repository Layout

```
.
├── pom.xml                               # Java 11, Hadoop 3.3.6, JUnit 5, JaCoCo, Shade plugin
├── conf/                                 # Authoritative cluster configurations
│   ├── core-site.xml                     # fs.defaultFS = hdfs://hdfs-namenode:9000
│   ├── hdfs-site.xml                     # 128 MB blocksize, replication 1
│   ├── yarn-site.xml                     # CapacityScheduler, resource limits
│   ├── mapred-site.xml                   # YARN framework, default queue "batch"
│   └── capacity-scheduler.xml            # default: 30%, batch: 70%
├── src/main/java/edu/bigdata/clickstream/ # MapReduce application source
├── src/test/resources/fixtures/          # mini_clickstream.csv test dataset
├── scripts/
│   ├── setup_environment.sh              # WSL2 / Linux setup script (Java 11 + Maven)
│   ├── download_dataset.md               # Kaggle REES46 acquisition instructions
│   ├── make_sample.sh                    # Extract 1M-row sample (sample_1m.csv)
│   ├── split_by_day.py                   # Streaming CSV partitioner (dt=YYYY-MM-DD)
│   ├── ingest_to_hdfs.sh                 # Upload partitions to HDFS /clickstream/raw/
│   ├── nightly_batch.sh                  # cron entrypoint (flock, exit codes)
│   ├── export_reports.py                 # HDFS outputs -> reports/<run_id>/ (8 CSVs)
│   └── verify_with_pandas.py             # Golden verification against pandas
├── dashboard/
│   └── app.py                            # Streamlit 5-view analytics dashboard
├── tests/
│   └── test_data_ingestion.py            # Automated tests for ingestion scripts
├── docker-compose.yml                    # HDFS + YARN container services
└── docs/                                 # PRD, SRS, status, plan, benchmarks
```

---

## 4. Quick Start

### Step 1: Environment Setup
For WSL2 / Ubuntu users (recommended):
```bash
# Run environment setup to install OpenJDK 11, Maven and Python tools
bash scripts/setup_environment.sh
```
See [docs/SETUP_ENVIRONMENT.md](docs/SETUP_ENVIRONMENT.md) for detailed configuration.

### Step 2: Download Dataset & Create Sample
Follow [scripts/download_dataset.md](scripts/download_dataset.md) to obtain `2019-Nov.csv` into `data/raw/`.
```bash
# Generate 1M-row sample for local testing
bash scripts/make_sample.sh data/raw/2019-Nov.csv

# Partition sample by day
python scripts/split_by_day.py --input data/sample/sample_1m.csv --out data/partitions
```

### Step 3: Run Automated Ingestion Tests
```bash
python tests/test_data_ingestion.py
```

### Step 4: Start Hadoop & YARN Cluster (Docker)
Ensure Docker Desktop is started:
```bash
docker compose up -d
docker compose ps
```

### Step 5: Upload Partitions to HDFS
```bash
bash scripts/ingest_to_hdfs.sh data/partitions
```

### Step 6: Execute PipelineDriver
```bash
# Direct execution of the complete pipeline (J1..J6 in dependency order)
hadoop jar target/clickstream-mr-1.0.jar edu.bigdata.clickstream.PipelineDriver \
  --run-id daily-2019-11-01 \
  --input /clickstream/raw/dt=2019-11-01 \
  --work-dir /clickstream/work/daily-2019-11-01 \
  --queue batch \
  --topn 10 \
  --reducers-traffic 4 \
  --reducers-session 8
```

### Step 7: Run Nightly Automation
```bash
# Run one nightly batch manually (acquires flock on /tmp/clickstream_batch.lock)
bash scripts/nightly_batch.sh --date 2019-11-01

# Run sequential backfill across a date range
bash scripts/nightly_batch.sh --backfill --from 2019-11-01 --to 2019-11-07

# Display or install the production 02:00 UTC crontab entry (FR-SCH-01)
bash scripts/nightly_batch.sh --install-cron
```