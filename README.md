
# Quick-Commerce Data Engineering Pipeline

This project implements a small-scale data pipeline that ingests raw operational data from CSV files, lands them in an Amazon Redshift cluster as staging tables, and transforms them into a clean dimensional model for business intelligence.

## 🛠 Configuration & Setup

Before running the scripts, ensure your environment is configured correctly.

### 1. Environment Variables (`.env`)
Create a `.env` file in the root directory. This file is excluded from version control for security. It should contain the following keys:
```env
REDSHIFT_HOST=your-cluster-endpoint
REDSHIFT_PORT=5439
REDSHIFT_DB=your-db-name
REDSHIFT_USER=your-username
REDSHIFT_PASSWORD=your-password
REDSHIFT_SCHEMA=your-assigned-scratch-schema
```

### 2. Folder Structure
The scripts expect the following directory layout:
* `/data_files`: Contains the five raw CSVs (`orders_week1.csv`, `sku_master.csv`, etc.).
* `/sql`: Contains the `.sql` transformation files for  Staging to Live tables 

---

## 🚀 How to Run the Pipeline

The pipeline is split into two phases to ensure a clear separation between ingestion and transformation.

### Step 1: Ingestion
Run the ingestion script to clean raw CSV headers and load them into staging tables (prefixed with `stg_`).
```bash
python load_csv_to_tables.py
```
* **Idempotency**: This script uses `if_exists='replace'`, meaning you can run it multiple times without creating duplicate data.

### Step 2: Transformation
Run the transformation script to execute your SQL models and build the star schema.
```bash
python staging_to_live_tables.py
```
* **Execution Order**: The script sorts files alphabetically. Ensure your files are named sequentially (e.g., `01_dim_sku.sql`, `02_fct_orders.sql`).

---

## 📈 Data Quality & Modeling

### Data Quality Issues Found
* **Schema Inconsistency**: `orders_week1` and `orders_week2` had different schemas. The pipeline dynamically adds missing columns (like `promo`) to ensure they can be merged.
* **Dirty Naming**: Source files used inconsistent casing and spaces (e.g., "Store ID" vs "store_id"). The `clean_column_names` function standardizes all headers to lowercase and replaces special characters with underscores.
* **Duplicate Keys**: Master data files are not guaranteed to have clean primary keys. I handled the `store_master` duplicates using SQL window functions (`ROW_NUMBER()`) in the transformation layer.

### Modeling Choices
* **Star Schema**: I built a star schema featuring a central `fct_order_lines` table surrounded by `dim_store` and `dim_sku` for efficient querying.
* **Redshift Optimization**: 
    * Used **DISTSTYLE ALL** for small dimension tables to eliminate network shuffles.
    * Used **SORTKEY** on date columns to optimize performance for time-window queries (Apr 1 – Apr 14).

---

## 💡 Assumptions & Future Work

### Assumptions Made
* **Timezones**: All UTC timestamps were shifted by **+5:30** to reflect IST business hours.
* **Orphaned Records**: Orders that do not join to master data were kept in the fact table via `LEFT JOIN` to ensure total revenue is accurately captured, even if metadata is missing.


