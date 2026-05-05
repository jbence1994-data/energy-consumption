# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PySpark-based ETL data pipeline implementing the **Medallion Architecture** (Bronze → Silver → Gold) for energy consumption analysis. Data flows from a raw CSV through three processing layers into aggregated monthly analytics with tiered pricing calculations.

## Environment Setup

Dependencies are managed via `requirements.txt` (PySpark 3.5.7). A `.env` file (not committed) configures four path variables:

```
DATA=             # Path to source CSV file
MEDALLION_BRONZE= # Output path for bronze layer (raw Parquet)
MEDALLION_SILVER= # Output path for silver layer (processed Parquet, partitioned by year/month)
MEDALLION_GOLD=   # Output path for gold layer (aggregated Parquet)
```

Copy `.env.example` to `.env` and fill in local paths before running.

## Running the Pipeline

Each layer is run independently as a Python module from the project root:

```bash
python -m etl.bronze
python -m etl.silver
python -m etl.gold
```

Layers must be executed in order — each layer reads from the previous layer's output.

## Architecture

### Medallion Layers (`etl/`)

- **`bronze.py`** — Reads source CSV (with header/schema inference), writes raw data as Parquet to `MEDALLION_BRONZE`. No transformations.
- **`silver.py`** — Reads Bronze Parquet, computes `daily_kwh` via PySpark Window functions (`lead` on next day's cumulative kWh), drops nulls, writes partitioned by `year`/`month` to `MEDALLION_SILVER`.
- **`gold.py`** — Reads Silver Parquet, aggregates by `year`/`month` (sum, avg, min, max of `daily_kwh`), applies tiered pricing (first 210 kWh @ 36 HUF/kWh, remainder @ 70 HUF/kWh), writes to `MEDALLION_GOLD`.
- **`helpers.py`** — Shared utilities: `get_spark_session()`, `read_csv()`, `read_parquet()`, `write_parquet()`, `write_parquet_with_partitions()`. All ETL modules import from here.

### Data Flow

```
CSV (DATA) → bronze.py → Bronze Parquet → silver.py → Silver Parquet (partitioned) → gold.py → Gold Parquet
```

### Key Business Logic

The tiered pricing in `gold.py` uses a constant `MONTHLY_DISCOUNTED_LIMIT_KWH = 210` to split monthly consumption into discounted (36 HUF/kWh) and regular (70 HUF/kWh) tiers. Magic numbers for rates are defined as constants at the top of `gold.py`.