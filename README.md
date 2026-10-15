<div align="center">

# 💳 Credit Card Fraud — Data Engineering Pipeline

**End-to-end, Airflow-orchestrated batch pipeline: Kaggle ingestion → PySpark feature engineering → PostgreSQL fraud analytics, fully containerized.**

[![Python](https://img.shields.io/badge/Python-3.10-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-CeleryExecutor-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![PySpark](https://img.shields.io/badge/PySpark-ETL-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Redis](https://img.shields.io/badge/Redis-broker-DC382D?logo=redis&logoColor=white)](https://redis.io/)
[![Docker](https://img.shields.io/badge/Docker%20Compose-7%20services-2496ED?logo=docker&logoColor=white)](project-ccf/docker/docker-compose.yml)
[![Kaggle](https://img.shields.io/badge/Dataset-Kaggle%20ULB-20BEFF?logo=kaggle&logoColor=white)](https://www.kaggle.com/mlg-ulb/creditcardfraud)

</div>

---

## 🎯 Overview

The [ULB credit-card fraud dataset](https://www.kaggle.com/mlg-ulb/creditcardfraud) contains **284 807** European card transactions over two days, of which only **492 (0.17 %) are frauds** — 28 PCA components (`V1…V28`), `Time`, `Amount`, `Class`.

This project builds the **data platform** around it: reproducible daily ingestion, quality gates, Spark feature engineering, and an analytics-ready PostgreSQL schema that fraud analysts and ML models can query.

```mermaid
pie showData title Class balance (284 807 transactions)
    "Legitimate" : 284315
    "Fraud" : 492
```

---

## 🏗️ Architecture

```mermaid
flowchart LR
    K["☁️ Kaggle API"] --> I["📥 ingest.py<br/>download · validate · archive"]
    I --> RAW[("data/raw/")]
    RAW --> T["⚡ transform_spark.py<br/>quality checks · features · cleaning"]
    T --> PQ[("data/processed/<br/>Parquet")]
    PQ --> L["🐘 load_postgres.py<br/>tables · aggregates · indexes"]
    L --> PG[("PostgreSQL 15<br/>ccf_db")]
    PG --> BI["📊 pgAdmin · Jupyter<br/>analytics"]

    subgraph AF["🌀 Airflow (webserver · scheduler · Celery worker)"]
        direction TB
        D["DAG ccf_pipeline_dag · @daily"]
    end
    AF -. orchestrates .-> I & T & L
    R[("Redis")] --- AF
```

---

## 🔄 Airflow DAG

```mermaid
flowchart LR
    subgraph G1["Ingestion"]
        A1["ingest_credit_card_data"] --> A2["check_data_quality"]
    end
    subgraph G2["Transformation"]
        B1["transform_with_pyspark"] --> B2["validate_transformation"]
    end
    subgraph G3["Loading"]
        C1["load_to_postgresql"] --> C2["generate_pipeline_report"]
    end
    G1 --> G2 --> G3 --> CL["cleanup_temporary_files"] --> OK["📧 email_success_notification"]
    G1 & G2 & G3 -. on failure .-> KO["📧 email_failure_notification"]
```

---

## 🧪 Feature engineering (PySpark)

| Feature | Definition |
|---|---|
| `transaction_id` | `monotonically_increasing_id()` |
| `hour_of_day` | `(Time mod 86 400) / 3 600` |
| `day_of_dataset` | `Time / 86 400` |
| `amount_log` | `log(Amount)` when `Amount > 0` |
| `amount_category` | `zero` · `low ≤ 50` · `medium ≤ 200` · `high ≤ 1 000` · `very_high` |
| `amount_zscore` | `(Amount − μ) / σ` |
| `processed_at` | load timestamp |

---

## 🗃️ Warehouse

```mermaid
erDiagram
    CREDIT_CARD_TRANSACTIONS {
        bigint transaction_id PK
        float time_seconds
        float v1_v28 "28 PCA components"
        float amount
        int class "0 legit / 1 fraud"
        float amount_log
        float hour_of_day
        int day_of_dataset
        varchar amount_category
        float amount_zscore
        timestamp processed_at
    }
    FRAUD_SUMMARY {
        serial id PK
        date date_processed
        int total_transactions
        int fraud_transactions
        float fraud_rate
        float total_amount
        float fraud_amount
        float avg_amount_normal
        float avg_amount_fraud
    }
    HOURLY_FRAUD_STATS {
        serial id PK
        int hour_of_day
        int total_transactions
        int fraud_transactions
        float fraud_rate
        float avg_amount
        date date_processed
    }
    CREDIT_CARD_TRANSACTIONS ||--o{ FRAUD_SUMMARY : "aggregated daily"
    CREDIT_CARD_TRANSACTIONS ||--o{ HOURLY_FRAUD_STATS : "aggregated by hour"
```

Indexes on `class`, `hour_of_day`, `amount`, `processed_at` and the summary dates keep analyst queries fast.

---

## 🚀 Quick start

```bash
cd project-ccf
cp .env.template .env            # Postgres + Kaggle credentials
make setup                       # venv + dependencies
make start                       # Postgres, pgAdmin, Redis, Airflow (web/scheduler/worker), Jupyter
make airflow-trigger             # run the DAG
```

Or step by step without Airflow:

```bash
make pipeline                    # = make ingest transform load
make test                        # pytest (12 tests)
make db-connect                  # psql into ccf_db
```

| Service | URL |
|---|---|
| Airflow | http://localhost:8080 |
| pgAdmin | http://localhost:5050 |
| PostgreSQL | localhost:5432 |

---

## 📁 Project structure

```
project-ccf/
├── dags/ccf_pipeline_dag.py     # Task groups, quality gates, e-mail alerts
├── scripts/
│   ├── ingest.py                # Kaggle download, validation, archiving
│   ├── transform_spark.py       # Quality checks, feature engineering, Parquet
│   └── load_postgres.py         # Schema, bulk load, aggregates, indexes
├── docker/                      # docker-compose + Airflow image
├── tests/test_pipeline.py
├── Makefile                     # 30 developer commands
└── .env.template
```

---

<div align="center">

**Mohamed Abi** · [GitHub @mohamedabi17](https://github.com/mohamedabi17)

</div>
