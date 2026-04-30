# OBETA Python ETL — Reproducible Execution Guide

This project replaces the three Tableau Prep flows with a reproducible Python + MySQL ETL pipeline.

The project reproduces:

1. **Flow 1** — Raw CSV files → `obeta_staging`
2. **Flow 2** — Clean/transform staging data → `obeta_staging.pickoperations`
3. **Flow 3** — Build physical production star schema → `obeta_production.product`, `obeta_production.order`, `obeta_production.pick`

The final production model contains:

- `product` dimension table
- `order` dimension table
- `pick` fact table
- Physical foreign keys from `pick` to `product` and `order`

---

## 1. Project structure

The code is divided into modules to make the ETL easier to understand, maintain, and reproduce.

```text
obeta_python_batched_etl_package/
├── main.py
├── requirements_obeta_etl.txt
├── .env.example
├── .gitignore
└── obeta_etl/
    ├── __init__.py
    ├── config.py
    ├── logger.py
    ├── db.py
    ├── ddl.py
    ├── indexes.py
    ├── dataframe_writer.py
    ├── flow1_raw_to_staging.py
    ├── flow2_transform_staging.py
    ├── flow3_staging_to_production.py
    └── diagnostics.py
```

Main responsibility of each file:

```text
main.py                         CLI entry point: flow1, flow2, flow3, run-all, counts
obeta_etl/config.py             Database configuration and .env loading
obeta_etl/logger.py             Timestamped logging
obeta_etl/db.py                 MySQL connection and SQL helper functions
obeta_etl/ddl.py                Staging table creation
obeta_etl/indexes.py            Index creation helpers
obeta_etl/dataframe_writer.py   Batched pandas DataFrame inserts
obeta_etl/flow1_raw_to_staging.py
                                Flow 1: raw CSV files to staging
obeta_etl/flow2_transform_staging.py
                                Flow 2: staging transformation to pickoperations
obeta_etl/flow3_staging_to_production.py
                                Flow 3: production star schema generation
obeta_etl/diagnostics.py        Row count diagnostics
```

---

## 2. Requirements

Each teammate needs:

- Python 3.10+
- MySQL 8.x running locally or in Docker
- The three raw OBETA CSV files
- The project files:
  - `main.py`
  - `obeta_etl/`
  - `requirements_obeta_etl.txt`

Install Python dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements_obeta_etl.txt
```

If `requirements_obeta_etl.txt` does not exist, create it with:

```txt
pandas
mysql-connector-python
python-dotenv
```

Then run:

```bash
pip install -r requirements_obeta_etl.txt
```

---

## 3. Recommended MySQL setup

For local reproducibility, use MySQL with binary logs disabled because the OBETA dataset is large.

If using Docker Compose, add this to the MySQL service:

```yaml
command:
  - --skip-log-bin
```

If running MySQL manually, make sure the MySQL server is available on:

```text
host: 127.0.0.1
port: 3306
```

The pipeline automatically creates these databases if they do not exist:

```text
obeta_staging
obeta_production
```

---

## 4. Environment variables

Set your database connection variables before running the ETL.

Example for user `mpmd` with no password:

```bash
export DB_HOST=127.0.0.1
export DB_PORT=3306
export DB_USER=mpmd
export DB_PASSWORD=''
export STAGING_DB=obeta_staging
export PRODUCTION_DB=obeta_production
```

Example with password:

```bash
export DB_HOST=127.0.0.1
export DB_PORT=3306
export DB_USER=mpmd
export DB_PASSWORD='your_password'
export STAGING_DB=obeta_staging
export PRODUCTION_DB=obeta_production
```

You can also place the same values in a `.env` file.

Create it from the example file:

```bash
cp .env.example .env
```

Example `.env` content:

```env
DB_HOST=127.0.0.1
DB_PORT=3306
DB_USER=mpmd
DB_PASSWORD=
STAGING_DB=obeta_staging
PRODUCTION_DB=obeta_production
```

---

## 5. Raw data file paths

The pipeline needs the three CSV files:

```text
001_warehouse_section.csv
002_product_data.csv
003_pick_data.csv
```

Example macOS paths:

```text
/Users/<your-user>/Downloads/OBETA_Tableau_Prep_Files_And_Data/RAW Data/001_warehouse_section.csv
/Users/<your-user>/Downloads/OBETA_Tableau_Prep_Files_And_Data/RAW Data/002_product_data.csv
/Users/<your-user>/Downloads/OBETA_Tableau_Prep_Files_And_Data/RAW Data/003_pick_data.csv
```

Check your file names first:

```bash
ls -lh "/Users/<your-user>/Downloads/OBETA_Tableau_Prep_Files_And_Data/RAW Data/"
```

---

## 6. Run Flow 1 — Raw CSV files to staging

Flow 1 loads the raw CSV files into:

```text
obeta_staging.warehouse_sections
obeta_staging.product_data
obeta_staging.pick_data
```

Run:

```bash
python3 main.py flow1 \
  --warehouse-csv "/Users/<your-user>/Downloads/OBETA_Tableau_Prep_Files_And_Data/RAW Data/001_warehouse_section.csv" \
  --product-csv "/Users/<your-user>/Downloads/OBETA_Tableau_Prep_Files_And_Data/RAW Data/002_product_data.csv" \
  --pick-csv "/Users/<your-user>/Downloads/OBETA_Tableau_Prep_Files_And_Data/RAW Data/003_pick_data.csv" \
  --reset-output \
  --csv-chunk-size 50000 \
  --product-encoding latin1
```

Expected row counts:

```text
obeta_staging.warehouse_sections: 18
obeta_staging.product_data: 2,199,593
obeta_staging.pick_data: 33,888,990
```

Important: `--product-encoding latin1` is required because `002_product_data.csv` may fail with a Unicode decoding error using the default encoding.

---

## 7. Run Flow 2 — Transform staging into pickoperations

Flow 2 reads the staging tables, performs cleaning/transformation, and writes:

```text
obeta_staging.pickoperations
```

Run:

```bash
python3 main.py flow2 --reset-output
```

Expected row count:

```text
obeta_staging.pickoperations: 33,698,592
```

Flow 2 applies filters such as:

```text
pick_volume > 0
product_description IS NOT NULL
product_group IS NOT NULL
```

That is why `pickoperations` has fewer rows than raw `pick_data`.

---

## 8. Run Flow 3 — Build production star schema

Flow 3 creates the physical production tables:

```text
obeta_production.product
obeta_production.order
obeta_production.pick
```

Run:

```bash
python3 main.py flow3 --reset-output
```

Expected row counts:

```text
obeta_production.product: 142,771
obeta_production.order: 28,791,767
obeta_production.pick: 33,698,592
```

Flow 3 is optimized as follows:

- Python only orchestrates SQL execution.
- MySQL performs the heavy operations internally.
- No pandas processing is used for the 33M+ production fact rows.
- No `LIMIT/OFFSET` batching is used.
- The fact table is loaded before creating foreign keys.
- Indexes are created after the large production insert.
- Foreign keys are created at the end so the ER diagram shows relationships.
- Temporary helper lookup tables are dropped after the fact table is loaded.

Expected relationships:

```text
obeta_production.pick.product_key -> obeta_production.product.product_key
obeta_production.pick.order_key   -> obeta_production.order.order_key
```

---

## 9. Run all flows together

Only use this if you want to rebuild everything from zero.

```bash
python3 main.py run-all \
  --warehouse-csv "/Users/<your-user>/Downloads/OBETA_Tableau_Prep_Files_And_Data/RAW Data/001_warehouse_section.csv" \
  --product-csv "/Users/<your-user>/Downloads/OBETA_Tableau_Prep_Files_And_Data/RAW Data/002_product_data.csv" \
  --pick-csv "/Users/<your-user>/Downloads/OBETA_Tableau_Prep_Files_And_Data/RAW Data/003_pick_data.csv" \
  --reset-output \
  --csv-chunk-size 50000 \
  --product-encoding latin1
```

---

## 10. Check row counts

At any time, run:

```bash
python3 main.py counts
```

Final expected result:

```text
obeta_staging.warehouse_sections: 18
obeta_staging.product_data: 2,199,593
obeta_staging.pick_data: 33,888,990
obeta_staging.pickoperations: 33,698,592
obeta_production.product: 142,771
obeta_production.order: 28,791,767
obeta_production.pick: 33,698,592
```

---

## 11. Verify foreign keys

Run this from the terminal:

```bash
python3 - <<'PY'
import os
import mysql.connector

conn = mysql.connector.connect(
    host=os.getenv("DB_HOST", "127.0.0.1"),
    port=int(os.getenv("DB_PORT", "3306")),
    user=os.getenv("DB_USER", "mpmd"),
    password=os.getenv("DB_PASSWORD", ""),
    database=os.getenv("PRODUCTION_DB", "obeta_production"),
)

cur = conn.cursor()
cur.execute("""
    SELECT
        constraint_name,
        table_name,
        column_name,
        referenced_table_name,
        referenced_column_name
    FROM information_schema.KEY_COLUMN_USAGE
    WHERE table_schema = DATABASE()
      AND table_name = 'pick'
      AND referenced_table_name IS NOT NULL
    ORDER BY constraint_name;
""")

for row in cur.fetchall():
    print(row)

cur.close()
conn.close()
PY
```

Expected output:

```text
('fk_pick_order', 'pick', 'order_key', 'order', 'order_key')
('fk_pick_product', 'pick', 'product_key', 'product', 'product_key')
```

---

This project is reproducible because:

- All transformations are inside the Python package.
- The pipeline creates required databases automatically.
- The pipeline creates required staging and production tables automatically.
- The pipeline creates indexes automatically.
- The pipeline creates physical foreign keys automatically.
- The team only needs the same raw files and MySQL access.

The final pipeline replaces the manual Tableau Prep execution while preserving the same logical ETL architecture:

```text
raw data → staging → cleaned staging → production star schema
```