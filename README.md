# Retail Sales Lakehouse Analytics Platform

> End-to-end production-style data pipeline: automated ingestion,
> medallion architecture transformation, PostgreSQL warehouse,
> and Tableau dashboards.

Designed using Medallion Architecture (Bronze → Silver → Gold)
following modern data engineering best practices.

---

## Architecture

Raw CSV Files
↓
Bronze Layer (Parquet, partitioned, audit metadata)
↓
Silver Layer (cleaned, validated, quarantine pattern)
↓
Gold Layer (KPI marts, aggregated, business-ready)
↓
PostgreSQL Warehouse (indexed, query-optimized)
↓
Tableau Dashboards (5 business dashboards)

---

## Tech Stack

| Tool | Version | Purpose |
|---|---|---|
| Python | 3.11 | Core language |
| PySpark | 3.5.0 | Distributed transformations |
| Apache Airflow | 2.8.1 | Pipeline orchestration |
| PostgreSQL | 15 | Analytical warehouse |
| Parquet / Snappy | — | Columnar storage format |
| Tableau | Desktop/Public | Business dashboards |

---

## Quick Start

```bash
git clone https://github.com/Avanijain09/retail-lakehouse.git
cd retail-lakehouse

python -m venv venv
source venv/bin/activate

pip install -r requirements.txt

cp .env.example .env

# Fill PostgreSQL credentials in .env
python verify_setup.py
```

---

## Current Status

### Completed
- Environment setup
- PySpark configuration
- PostgreSQL warehouse setup
- GitHub integration
- Project structure initialization

### In Progress
- Fake retail dataset generation
- Bronze ingestion pipeline
- Silver transformations
- Gold KPI marts

---

## Project Structure

```text
retail-lakehouse/
├── data/               # Data lake (raw/bronze/silver/gold)
├── pyspark_jobs/       # Transformation scripts per layer
├── dags/               # Airflow pipeline DAGs
├── postgres/           # DDL + loading scripts
├── quality_checks/     # Automated data quality framework
├── ml/                 # Forecasting and anomaly detection
├── dashboards/         # Tableau screenshots and workbook
├── docs/               # Architecture, data dictionary, KPI definitions
└── tests/              # Unit and integration tests
```

---

## Dashboards

Dashboard screenshots will be added after Tableau integration.

---

## Documentation

- [Architecture diagram](docs/architecture.png)
- [Data dictionary](docs/data_dictionary.md)
- [KPI definitions](docs/kpi_definitions.md)
- [Pipeline runbook](docs/pipeline_runbook.md)
```