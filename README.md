# Enterprise Operations Intelligence Platform

End-to-end BI platform: raw CSVs → cleaned & validated data → PostgreSQL → dbt star schema for reporting and product analysis.

---

## Overview

This repo runs a full analytics pipeline:

1. **Ingest** raw CSV files (regions, customers, products, stores, orders).
2. **Clean & validate** with Python (standardized codes, deduping, null handling, referential integrity).
3. **Load** into PostgreSQL (`raw` schema).
4. **Transform** with dbt into staging views and mart tables (dimensions + facts).

You get a star schema ready for dashboards and ad-hoc analysis (e.g. by product, store, region, customer, date).

---

## Features

- **Python pipeline**: Cleaning, validation, and load-to-DB with configurable `.env`.
- **Standardized IDs**: `cust-*`, `store-*`, `prd-*`, `ord-*`; sentinels (`cust-0`, `store-0`, `prd-0`) for nulls.
- **Validation**: Schema checks, nulls, dtypes, uniqueness, and foreign keys before load.
- **dbt models**: Staging (from `raw`), dimensions (customer, product, store, region, date), and order-level / order-product fact tables.
- **Tests**: dbt schema and data tests (uniqueness, relationships, not_null).
- **Dashboard data SQL**: Standalone queries in `dashboard_data/` for exporting or wiring BI tools to mart tables.

---

## Prerequisites

- **Python 3.9+** (recommend a venv)
- **PostgreSQL** (for load and dbt)
- **dbt-core** and **dbt-postgres** (installed via `requirements.txt`)

---

## Project Structure

```
.
├── data/
│   ├── raw/          # Input CSVs: raw_regions.csv, raw_orders.csv, raw_products.csv, raw_stores.csv, raw_customers.csv
│   └── cleaned/      # Output CSVs after cleaning (used by validation + load_to_db)
├── python/
│   ├── pipeline.py       # Full pipeline: run_cleaning → run_validation → load_to_db
│   ├── run_cleaning.py   # Clean all raw CSVs → data/cleaned/
│   ├── validation.py    # Validate cleaned CSVs (schemas, FKs, nulls, etc.)
│   ├── load_to_db.py    # Load data/cleaned/*.csv → PostgreSQL raw.*
│   ├── clean_*.py       # Per-entity cleaning (customers, orders, products, regions, stores)
│   └── standard_clean.py
├── models/
│   ├── sources/          # Source definitions (raw.customers, raw.orders, …)
│   ├── staging/         # stg_customers, stg_orders_line, stg_products, stg_stores, stg_regions
│   └── marts/
│       ├── dimensions/   # dim_customer, dim_product, dim_store, dim_region, dim_date
│       └── facts/       # fact_orders (order-product), fct_orders (order-level)
├── dashboard_data/      # Ad-hoc SQL to pull data for dashboards (run in psql / your BI tool against Postgres)
│   ├── Data_Pull.sql    # Example full data pull
│   └── Data_Pulled_prd.sql
├── dashboards/          # Dashboard exports or references (e.g. PDF mockups)
├── docs/
├── dbt_project.yml
├── profiles.yml.template # Copy to ~/.dbt/profiles.yml and set DB credentials
├── requirements.txt
└── .env                  # DATABASE_URL, DATA_DIR, RAW_SCHEMA, etc. (not committed)
```

---

## Setup

### 1. Clone and install Python dependencies

```bash
git clone <your-repo-url>
cd Enterprise_BI_Project_Repo

python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Raw data

Place your raw CSVs in `data/raw/` with names:

- `raw_regions.csv`
- `raw_customers.csv`
- `raw_products.csv`
- `raw_stores.csv`
- `raw_orders.csv`

(Column names should match what the cleaning scripts expect; see `python/clean_*.py` and `python/validation.py`.)

### 3. Environment and database

- Copy `profiles.yml.template` to `~/.dbt/profiles.yml` and set `host`, `user`, `password`, `dbname`, and `schema` for your PostgreSQL instance.
- Create a `.env` in the project root (see `python/load_to_db.py` and `python/validation.py`). At minimum:

  ```env
  DATABASE_URL=postgresql://user:password@localhost:5432/EnterpriseBI
  ```

- Ensure the database and `raw` schema exist (the load script can create the schema).

---

## Usage

### Run the full Python pipeline (clean → validate → load)

From the **project root** (so paths like `data/cleaned` resolve correctly):

```bash
source .venv/bin/activate
python python/pipeline.py
```

- Writes cleaned CSVs to `data/cleaned/`.
- Validates them; if validation fails, the pipeline stops before load.
- Loads `data/cleaned/*.csv` into PostgreSQL `raw.*` tables.

To run only cleaning:

```bash
python python/run_cleaning.py
```

### Run dbt (staging + marts)

After the pipeline has loaded data into `raw`:

```bash
dbt run
```

Run or test specific models:

```bash
dbt run --select stg_orders_line fact_orders
dbt test --select fact_orders
```

Useful selects:

- `dbt run --select staging.*` — all staging views
- `dbt run --select marts.*` — all dimension and fact tables
- `dbt build` — run models and then run tests

---

## dbt Models

| Layer      | Models | Description |
|-----------|--------|-------------|
| **Staging** | `stg_customers`, `stg_orders_line`, `stg_products`, `stg_stores`, `stg_regions` | One-to-one or cleaned views from `raw.*`; standardized column names (e.g. `order_id` / `order_code` in SQL). |
| **Dimensions** | `dim_customer`, `dim_product`, `dim_store`, `dim_region`, `dim_date` | Deduplicated dimension tables; `dim_date` is a generated date spine. |
| **Facts** | `fact_orders`, `fct_orders` | **fact_orders**: one row per order + product (for product analysis). **fct_orders**: one row per order (order-level metrics). Both built from `stg_orders_line`. |

Staging uses schema `staging`; marts use schema `mart` (see `dbt_project.yml`).

---

## Dashboard data (`dashboard_data/`)

These `.sql` files are **not** dbt models. They are queries you run manually against PostgreSQL after `dbt run` (or equivalent), usually targeting **mart** schemas (e.g. `staging_mart`, `mart`) depending on your `profiles.yml` and `dbt_project.yml` settings.

- Point your SQL client or BI **data source** at the same database you use for dbt.
- Open a script under `dashboard_data/`, adjust schema names (`staging_mart`, `staging`, etc.) if they differ on your machine.
- Execute the query and save results as CSV or connect the BI tool with the same SQL as a **custom SQL** dataset.

The `dashboards/` folder can hold exported PDFs or other assets for design references—not executed by the pipeline.

---

## Documentation

- **DBT_QUICK_REFERENCE.md** — dbt commands and concepts.
- **DBT_SETUP_GUIDE.md** — Detailed dbt setup and project config.
- **INSTALL_DBT.md** — Installing dbt.
- **docs/metrics.md** — Metric definitions and usage.

---

## License

See repository license file.
