#!/usr/bin/env python3
"""
Credit Card Fraud Detection - Airflow DAG
Orchestre le pipeline de données end-to-end
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
import os
import sys

# Add scripts directory to Python path
sys.path.append('/opt/airflow/scripts')

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# DAG definition
dag = DAG(
    'credit_card_fraud_pipeline',
    default_args=default_args,
    description='Credit Card Fraud Detection Data Pipeline',
    schedule_interval='@daily',  # Run daily
    catchup=False,
    max_active_runs=1,
    tags=['data-engineering', 'fraud-detection', 'etl'],
)

def check_data_quality(**context):
    """Vérifie la qualité des données après ingestion"""
    import pandas as pd
    from pathlib import Path
    
    raw_data_path = Path('./data/raw/creditcard.csv')
    
    if not raw_data_path.exists():
        raise FileNotFoundError(f"Raw data file not found: {raw_data_path}")
    
    # Basic checks
    df = pd.read_csv(raw_data_path)
    
    # Check row count
    if len(df) < 100000:  # Expected minimum rows
        raise ValueError(f"Dataset too small: {len(df)} rows")
    
    # Check for required columns
    required_cols = ['Time', 'Amount', 'Class'] + [f'V{i}' for i in range(1, 29)]
    missing_cols = set(required_cols) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Check class distribution
    fraud_rate = df['Class'].mean()
    if fraud_rate > 0.1 or fraud_rate < 0.001:  # Expected fraud rate between 0.1% and 10%
        raise ValueError(f"Unusual fraud rate: {fraud_rate:.4f}")
    
    print(f"✅ Data quality checks passed: {len(df):,} rows, fraud rate: {fraud_rate:.4f}")
    return True

def validate_transformation(**context):
    """Valide les données transformées"""
    from pathlib import Path
    import pandas as pd
    
    processed_path = Path('./data/processed/credit_card_processed_csv')
    csv_files = list(processed_path.glob('*.csv'))
    
    if not csv_files:
        raise FileNotFoundError(f"No processed CSV files found in {processed_path}")
    
    df = pd.read_csv(csv_files[0])
    
    # Check new columns were added
    expected_new_cols = ['transaction_id', 'amount_log', 'hour_of_day', 'amount_category']
    missing_new_cols = set(expected_new_cols) - set(df.columns)
    if missing_new_cols:
        raise ValueError(f"Missing engineered features: {missing_new_cols}")
    
    print(f"✅ Transformation validation passed: {len(df):,} rows with engineered features")
    return True

def send_pipeline_report(**context):
    """Envoie un rapport de pipeline"""
    from sqlalchemy import create_engine, text
    import os
    
    # Database connection
    db_config = {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'database': os.getenv('POSTGRES_DB', 'ccf_db'),
        'user': os.getenv('POSTGRES_USER', 'ccf_user'),
        'password': os.getenv('POSTGRES_PASSWORD', 'ccf_password')
    }
    
    conn_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    engine = create_engine(conn_string)
    
    with engine.connect() as conn:
        # Get today's summary
        result = conn.execute(text("""
            SELECT * FROM fraud_summary 
            WHERE date_processed = CURRENT_DATE 
            ORDER BY created_at DESC 
            LIMIT 1
        """)).fetchone()
        
        if result:
            report = f"""
            📊 Credit Card Fraud Detection - Daily Report
            Date: {result[1]}
            Total Transactions: {result[2]:,}
            Fraud Transactions: {result[3]:,}
            Fraud Rate: {result[4]:.4f}%
            Total Amount: ${result[5]:,.2f}
            Fraud Amount: ${result[6]:,.2f}
            """
            print(report)
            
            # Store report for email task
            context['task_instance'].xcom_push(key='daily_report', value=report)
        else:
            raise ValueError("No daily summary found in database")
    
    return True

# Task Groups for better organization
with TaskGroup("data_ingestion", dag=dag) as ingestion_group:
    
    # Data ingestion task
    ingest_data = BashOperator(
        task_id='ingest_credit_card_data',
        bash_command='cd /opt/airflow && python scripts/ingest.py',
        dag=dag,
    )
    
    # Data quality check
    quality_check = PythonOperator(
        task_id='check_data_quality',
        python_callable=check_data_quality,
        dag=dag,
    )
    
    ingest_data >> quality_check

with TaskGroup("data_transformation", dag=dag) as transformation_group:
    
    # PySpark transformation
    transform_data = BashOperator(
        task_id='transform_with_pyspark',
        bash_command='cd /opt/airflow && python scripts/transform_spark.py',
        dag=dag,
    )
    
    # Transformation validation
    validate_transform = PythonOperator(
        task_id='validate_transformation',
        python_callable=validate_transformation,
        dag=dag,
    )
    
    transform_data >> validate_transform

with TaskGroup("data_loading", dag=dag) as loading_group:
    
    # Load to PostgreSQL
    load_postgres = BashOperator(
        task_id='load_to_postgresql',
        bash_command='cd /opt/airflow && python scripts/load_postgres.py',
        dag=dag,
    )
    
    # Generate report
    generate_report = PythonOperator(
        task_id='generate_pipeline_report',
        python_callable=send_pipeline_report,
        dag=dag,
    )
    
    load_postgres >> generate_report

# Cleanup task
cleanup_temp_files = BashOperator(
    task_id='cleanup_temporary_files',
    bash_command='''
    # Clean up temporary files older than 7 days
    find /opt/airflow/data/archive -name "*.csv" -mtime +7 -delete
    find /opt/airflow/logs -name "*.log" -mtime +30 -delete
    echo "Cleanup completed"
    ''',
    dag=dag,
)

# Email notification on success
email_success = EmailOperator(
    task_id='email_success_notification',
    to=['data-team@company.com'],
    subject='✅ Credit Card Fraud Pipeline - Success',
    html_content='''
    <h3>Pipeline Execution Successful</h3>
    <p>The Credit Card Fraud Detection pipeline has completed successfully.</p>
    <p><strong>Execution Date:</strong> {{ ds }}</p>
    <p><strong>Duration:</strong> {{ (ti.end_date - ti.start_date) if ti.end_date else 'Running' }}</p>
    <p>Check the daily report for detailed statistics.</p>
    ''',
    dag=dag,
)

# Define task dependencies
ingestion_group >> transformation_group >> loading_group >> cleanup_temp_files >> email_success

# Add failure notification
email_failure = EmailOperator(
    task_id='email_failure_notification',
    to=['data-team@company.com'],
    subject='❌ Credit Card Fraud Pipeline - Failure',
    html_content='''
    <h3>Pipeline Execution Failed</h3>
    <p>The Credit Card Fraud Detection pipeline has failed.</p>
    <p><strong>Execution Date:</strong> {{ ds }}</p>
    <p><strong>Failed Task:</strong> {{ ti.task_id }}</p>
    <p>Please check the logs for more details.</p>
    ''',
    trigger_rule='one_failed',
    dag=dag,
)

# Make failure notification depend on all main tasks
[ingestion_group, transformation_group, loading_group] >> email_failure

if __name__ == "__main__":
    dag.cli()
