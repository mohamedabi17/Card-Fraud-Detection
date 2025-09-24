# Credit Card Fraud Detection - Data Engineering Pipeline

##  Objectif
Pipeline de Data Engineering end-to-end pour la détection de fraude par carte de crédit utilisant Python, PySpark, PostgreSQL et Apache Airflow.

##  Architecture

```
project-ccf/
├─ dags/                    # DAGs Airflow
│  └─ ccf_pipeline_dag.py
├─ scripts/                 # Scripts Python
│  ├─ ingest.py            # Ingestion des données
│  ├─ transform_spark.py   # Transformation PySpark
│  └─ load_postgres.py     # Chargement PostgreSQL
├─ data/                   # Data Lake local
│  ├─ raw/                 # Données brutes
│  ├─ processed/           # Données transformées
│  └─ archive/             # Archives
├─ sql/                    # Scripts SQL
├─ config/                 # Configuration
├─ docker/                 # Docker setup
├─ tests/                  # Tests unitaires
└─ requirements.txt
```

## Setup Rapide

### 1. Environnement virtuel
```bash
python -m venv venv
.\venv\Scripts\activate  # Windows
pip install -r requirements.txt
```

### 2. Configuration Kaggle
```bash
# Placer kaggle.json dans ~/.kaggle/
kaggle datasets download -d mlg-ulb/creditcardfraud
```

### 3. PostgreSQL avec Docker
```bash
cd docker
docker-compose up -d
```

### 4. Airflow
```bash
export AIRFLOW_HOME=$(pwd)
airflow db init
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
airflow scheduler &
airflow webserver -p 8080
```

##  Dataset
- **Source**: [Kaggle - Credit Card Fraud Detection](https://www.kaggle.com/mlg-ulb/creditcardfraud)
- **Taille**: ~150MB, 284,807 transactions
- **Features**: V1-V28 (PCA), Time, Amount, Class (0=Normal, 1=Fraud)

##  Pipeline Steps

1. **Ingestion** (`ingest.py`)
   - Download Kaggle dataset
   - Store in `data/raw/`

2. **Transformation** (`transform_spark.py`)
   - PySpark processing
   - Feature engineering
   - Data cleaning
   - Save as Parquet in `data/processed/`

3. **Loading** (`load_postgres.py`)
   - Load processed data to PostgreSQL
   - Create aggregation tables

4. **Orchestration** (Airflow DAG)
   - Schedule daily runs
   - Error handling & monitoring

## 📈 Monitoring & Quality
- Data quality checks avec Great Expectations
- Logging avec Loguru
- Tests avec pytest

##  Execution

### Manuel
```bash
# 1. Ingestion
python scripts/ingest.py

# 2. Transformation
python scripts/transform_spark.py

# 3. Loading
python scripts/load_postgres.py
```

### Avec Airflow
- Interface: http://localhost:8080
- DAG: `ccf_pipeline_dag`

## 🔧 Configuration
Variables d'environnement dans `.env`:
```
POSTGRES_HOST=localhost
POSTGRES_DB=ccf_db
POSTGRES_USER=ccf_user
POSTGRES_PASSWORD=ccf_password
KAGGLE_USERNAME=your_username
KAGGLE_KEY=your_key
```
