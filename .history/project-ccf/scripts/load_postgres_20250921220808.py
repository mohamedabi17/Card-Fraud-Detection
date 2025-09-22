#!/usr/bin/env python3
"""
Credit Card Fraud Detection - PostgreSQL Loading Script
Charge les données transformées dans PostgreSQL
"""

import os
import pandas as pd
from pathlib import Path
from datetime import datetime
from loguru import logger
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Load environment variables
load_dotenv()

class PostgreSQLLoader:
    def __init__(self):
        self.processed_data_path = Path(os.getenv('PROCESSED_DATA_PATH', './data/processed'))
        
        # Database configuration
        self.db_config = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'database': os.getenv('POSTGRES_DB', 'ccf_db'),
            'user': os.getenv('POSTGRES_USER', 'ccf_user'),
            'password': os.getenv('POSTGRES_PASSWORD', 'ccf_password')
        }
        
        # Setup logging
        logger.add(
            "./logs/load_{time:YYYY-MM-DD}.log",
            rotation="1 day",
            retention="30 days",
            level="INFO"
        )
        
        self.engine = None

    def create_database_if_not_exists(self):
        """Crée la base de données si elle n'existe pas"""
        try:
            # Connect to postgres database to create our target database
            conn_string = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/postgres"
            
            conn = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database='postgres'
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            
            cursor = conn.cursor()
            
            # Check if database exists
            cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{self.db_config['database']}'")
            exists = cursor.fetchone()
            
            if not exists:
                cursor.execute(f"CREATE DATABASE {self.db_config['database']}")
                logger.info(f"✅ Database '{self.db_config['database']}' created")
            else:
                logger.info(f"ℹ️ Database '{self.db_config['database']}' already exists")
            
            cursor.close()
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create database: {e}")
            return False

    def create_connection(self):
        """Crée la connexion à PostgreSQL"""
        try:
            conn_string = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            self.engine = create_engine(conn_string)
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info("✅ PostgreSQL connection established")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to PostgreSQL: {e}")
            return False

    def create_tables(self):
        """Crée les tables nécessaires"""
        try:
            logger.info("🏗️ Creating database tables")
            
            # Main transactions table
            create_transactions_table = """
            CREATE TABLE IF NOT EXISTS credit_card_transactions (
                transaction_id BIGINT PRIMARY KEY,
                time_seconds FLOAT,
                v1 FLOAT, v2 FLOAT, v3 FLOAT, v4 FLOAT, v5 FLOAT,
                v6 FLOAT, v7 FLOAT, v8 FLOAT, v9 FLOAT, v10 FLOAT,
                v11 FLOAT, v12 FLOAT, v13 FLOAT, v14 FLOAT, v15 FLOAT,
                v16 FLOAT, v17 FLOAT, v18 FLOAT, v19 FLOAT, v20 FLOAT,
                v21 FLOAT, v22 FLOAT, v23 FLOAT, v24 FLOAT, v25 FLOAT,
                v26 FLOAT, v27 FLOAT, v28 FLOAT,
                amount FLOAT,
                class INTEGER,
                amount_log FLOAT,
                hour_of_day FLOAT,
                day_of_dataset INTEGER,
                amount_category VARCHAR(20),
                amount_zscore FLOAT,
                processed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            # Fraud summary table
            create_fraud_summary_table = """
            CREATE TABLE IF NOT EXISTS fraud_summary (
                id SERIAL PRIMARY KEY,
                date_processed DATE,
                total_transactions INTEGER,
                fraud_transactions INTEGER,
                fraud_rate FLOAT,
                total_amount FLOAT,
                fraud_amount FLOAT,
                avg_amount_normal FLOAT,
                avg_amount_fraud FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            # Hourly fraud statistics
            create_hourly_stats_table = """
            CREATE TABLE IF NOT EXISTS hourly_fraud_stats (
                id SERIAL PRIMARY KEY,
                hour_of_day INTEGER,
                total_transactions INTEGER,
                fraud_transactions INTEGER,
                fraud_rate FLOAT,
                avg_amount FLOAT,
                date_processed DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            with self.engine.connect() as conn:
                conn.execute(text(create_transactions_table))
                conn.execute(text(create_fraud_summary_table))
                conn.execute(text(create_hourly_stats_table))
                conn.commit()
            
            logger.info("✅ Database tables created successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create tables: {e}")
            return False

    def load_processed_data(self):
        """Charge les données transformées"""
        try:
            # Look for processed CSV data
            csv_path = self.processed_data_path / "credit_card_processed_csv"
            csv_files = list(csv_path.glob("*.csv"))
            
            if not csv_files:
                logger.error(f"❌ No processed CSV files found in {csv_path}")
                return False
            
            # Read the CSV file (should be only one)
            csv_file = csv_files[0]
            logger.info(f"📊 Loading data from {csv_file}")
            
            # Load data in chunks to handle large files
            chunk_size = 10000
            total_rows = 0
            
            for chunk_df in pd.read_csv(csv_file, chunksize=chunk_size):
                # Clean column names (remove special characters)
                chunk_df.columns = chunk_df.columns.str.lower().str.replace('[^a-zA-Z0-9]', '_', regex=True)
                
                # Map DataFrame columns to database columns
                column_mapping = {
                    'time': 'time_seconds',
                    'class': 'class'
                }
                chunk_df = chunk_df.rename(columns=column_mapping)
                
                # Ensure required columns exist
                required_columns = ['transaction_id', 'class']
                for col in required_columns:
                    if col not in chunk_df.columns:
                        logger.error(f"❌ Required column '{col}' not found in data")
                        return False
                
                # Load chunk to database
                chunk_df.to_sql(
                    'credit_card_transactions',
                    self.engine,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                
                total_rows += len(chunk_df)
                logger.info(f"📊 Loaded {total_rows:,} rows so far...")
            
            logger.info(f"✅ Successfully loaded {total_rows:,} transactions")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to load processed data: {e}")
            return False

    def create_aggregations(self):
        """Crée des tables d'agrégation"""
        try:
            logger.info("📊 Creating aggregation tables")
            
            # Overall fraud summary
            fraud_summary_query = """
            INSERT INTO fraud_summary (
                date_processed, total_transactions, fraud_transactions, 
                fraud_rate, total_amount, fraud_amount, 
                avg_amount_normal, avg_amount_fraud
            )
            SELECT 
                CURRENT_DATE as date_processed,
                COUNT(*) as total_transactions,
                SUM(CASE WHEN class = 1 THEN 1 ELSE 0 END) as fraud_transactions,
                AVG(CASE WHEN class = 1 THEN 1.0 ELSE 0.0 END) as fraud_rate,
                SUM(amount) as total_amount,
                SUM(CASE WHEN class = 1 THEN amount ELSE 0 END) as fraud_amount,
                AVG(CASE WHEN class = 0 THEN amount END) as avg_amount_normal,
                AVG(CASE WHEN class = 1 THEN amount END) as avg_amount_fraud
            FROM credit_card_transactions
            WHERE processed_at::date = CURRENT_DATE;
            """
            
            # Hourly fraud statistics
            hourly_stats_query = """
            INSERT INTO hourly_fraud_stats (
                hour_of_day, total_transactions, fraud_transactions, 
                fraud_rate, avg_amount, date_processed
            )
            SELECT 
                FLOOR(hour_of_day) as hour_of_day,
                COUNT(*) as total_transactions,
                SUM(CASE WHEN class = 1 THEN 1 ELSE 0 END) as fraud_transactions,
                AVG(CASE WHEN class = 1 THEN 1.0 ELSE 0.0 END) as fraud_rate,
                AVG(amount) as avg_amount,
                CURRENT_DATE as date_processed
            FROM credit_card_transactions
            WHERE processed_at::date = CURRENT_DATE
            GROUP BY FLOOR(hour_of_day)
            ORDER BY hour_of_day;
            """
            
            with self.engine.connect() as conn:
                # Clear existing data for today
                conn.execute(text("DELETE FROM fraud_summary WHERE date_processed = CURRENT_DATE"))
                conn.execute(text("DELETE FROM hourly_fraud_stats WHERE date_processed = CURRENT_DATE"))
                
                # Insert new aggregations
                conn.execute(text(fraud_summary_query))
                conn.execute(text(hourly_stats_query))
                conn.commit()
            
            logger.info("✅ Aggregation tables created successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create aggregations: {e}")
            return False

    def create_indexes(self):
        """Crée des index pour améliorer les performances"""
        try:
            logger.info("⚡ Creating database indexes")
            
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_transactions_class ON credit_card_transactions (class);",
                "CREATE INDEX IF NOT EXISTS idx_transactions_hour ON credit_card_transactions (hour_of_day);",
                "CREATE INDEX IF NOT EXISTS idx_transactions_amount ON credit_card_transactions (amount);",
                "CREATE INDEX IF NOT EXISTS idx_transactions_processed_at ON credit_card_transactions (processed_at);",
                "CREATE INDEX IF NOT EXISTS idx_fraud_summary_date ON fraud_summary (date_processed);",
                "CREATE INDEX IF NOT EXISTS idx_hourly_stats_date ON hourly_fraud_stats (date_processed);"
            ]
            
            with self.engine.connect() as conn:
                for index_query in indexes:
                    conn.execute(text(index_query))
                conn.commit()
            
            logger.info("✅ Database indexes created successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create indexes: {e}")
            return False

    def validate_data_load(self):
        """Valide le chargement des données"""
        try:
            logger.info("🔍 Validating data load")
            
            with self.engine.connect() as conn:
                # Check total count
                result = conn.execute(text("SELECT COUNT(*) FROM credit_card_transactions")).fetchone()
                total_count = result[0]
                logger.info(f"📊 Total transactions loaded: {total_count:,}")
                
                # Check fraud distribution
                result = conn.execute(text("""
                    SELECT class, COUNT(*) as count, 
                           ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
                    FROM credit_card_transactions 
                    GROUP BY class 
                    ORDER BY class
                """)).fetchall()
                
                logger.info("📊 Class distribution:")
                for row in result:
                    logger.info(f"   Class {row[0]}: {row[1]:,} ({row[2]}%)")
                
                # Check for today's aggregations
                result = conn.execute(text("""
                    SELECT COUNT(*) FROM fraud_summary WHERE date_processed = CURRENT_DATE
                """)).fetchone()
                
                if result[0] > 0:
                    logger.info("✅ Fraud summary data available")
                else:
                    logger.warning("⚠️ No fraud summary data found")
            
            logger.info("✅ Data validation completed")
            return True
            
        except Exception as e:
            logger.error(f"❌ Data validation failed: {e}")
            return False

    def run_loading(self):
        """Execute le processus de chargement complet"""
        logger.info("🚀 Starting PostgreSQL data loading")
        
        try:
            # Create database if needed
            if not self.create_database_if_not_exists():
                return False
            
            # Create connection
            if not self.create_connection():
                return False
            
            # Create tables
            if not self.create_tables():
                return False
            
            # Load processed data
            if not self.load_processed_data():
                return False
            
            # Create aggregations
            if not self.create_aggregations():
                return False
            
            # Create indexes
            if not self.create_indexes():
                return False
            
            # Validate data load
            if not self.validate_data_load():
                return False
            
            logger.info("✅ PostgreSQL loading completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"💥 Loading failed: {e}")
            return False

def main():
    """Point d'entrée principal"""
    loader = PostgreSQLLoader()
    
    try:
        success = loader.run_loading()
        if success:
            logger.info("🎉 Loading process completed successfully")
        else:
            logger.error("💥 Loading process failed")
            exit(1)
            
    except Exception as e:
        logger.error(f"💥 Unexpected error during loading: {e}")
        exit(1)

if __name__ == "__main__":
    main()
