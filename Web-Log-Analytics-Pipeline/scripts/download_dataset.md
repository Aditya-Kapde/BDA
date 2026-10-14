# Dataset Acquisition Guide
## Project #53: Batch MapReduce Clickstream Log Analysis

The authoritative dataset for this project is the **eCommerce behavior data from multi-category store** published by REES46 on Kaggle.

- **Kaggle URL:** [https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
- **Kaggle Slug:** `mkechinov/ecommerce-behavior-data-from-multi-category-store`
- **Data Volume:** Approximately 14 GB uncompressed (~100M+ events total).
- **Target Files:**
  - `data/raw/2019-Oct.csv` (~5.6 GB)
  - `data/raw/2019-Nov.csv` (~9.0 GB)

> [!WARNING]
> **Data Protection Rules:**
> 1. Raw CSV data files and generated partitions must **NEVER** be committed to Git.
> 2. `data/raw/`, `data/partitions/`, and `data/sample/` are excluded via `.gitignore`.
> 3. Never commit Kaggle API tokens (`kaggle.json`) or credentials to this repository.

---

## Method 1: Kaggle CLI (Recommended)

### 1. Set Up Kaggle Credentials
Create your Kaggle API token on Kaggle (Account -> API -> Create New API Token) and place it at:
- **Linux/WSL2:** `~/.kaggle/kaggle.json` (ensure permissions: `chmod 600 ~/.kaggle/kaggle.json`)
- **Windows:** `C:\Users\<Username>\.kaggle\kaggle.json`

### 2. Download and Unpack
```bash
# Create raw data directory
mkdir -p data/raw

# Download dataset archive
kaggle datasets download -d mkechinov/ecommerce-behavior-data-from-multi-category-store -p data/raw

# Unzip the CSV files
unzip data/raw/ecommerce-behavior-data-from-multi-category-store.zip -d data/raw

# Clean up archive
rm -f data/raw/ecommerce-behavior-data-from-multi-category-store.zip
```

---

## Method 2: Manual Download

1. Log into your Kaggle account in your web browser.
2. Navigate to [mkechinov/ecommerce-behavior-data-from-multi-category-store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store).
3. Click **Download (4.3 GB .zip)**.
4. Extract `2019-Oct.csv` and `2019-Nov.csv` directly into the `data/raw/` directory in this repository.

---

## Dataset Schema (9 Columns)

| # | Column Name | Type | Description |
|---|---|---|---|
| 1 | `event_time` | string | Format `yyyy-MM-dd HH:mm:ss UTC` |
| 2 | `event_type` | string | One of: `view`, `cart`, `remove_from_cart`, `purchase` |
| 3 | `product_id` | long | Unique product identifier |
| 4 | `category_id` | long | Unique category identifier |
| 5 | `category_code` | string | Taxonomy dot-path (e.g. `electronics.smartphone`). May be empty. |
| 6 | `brand` | string | Lower-case brand name. May be empty. |
| 7 | `price` | decimal | Non-negative price in USD. |
| 8 | `user_id` | long | Unique user identifier. |
| 9 | `user_session` | string | UUID representing a continuous user session. |

---

## Attribution & Citation
Data provided by **REES46 Open CDP** via Kaggle.
Licence: CC BY 4.0. Citing paper/source: *eCommerce behavior data from multi-category store*, REES46 / M. Kechinov.
