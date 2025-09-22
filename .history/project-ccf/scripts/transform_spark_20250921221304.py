#!/usr/bin/env python3
"""
Credit Card Fraud Detection - PySpark Transformation Script
Transforme et nettoie les données avec PySpark
"""

import os
from pathlib import Path
from datetime import datetime
from loguru import logger
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, isnan, isnull, count, mean, stddev, 
    percentile_approx, monotonically_increasing_id,
    current_timestamp, lit
)
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType

# Load environment variables
load_dotenv()

class SparkTransformer:
    def __init__(self):
        self.raw_data_path = Path(os.getenv('RAW_DATA_PATH', './data/raw'))
        self.processed_data_path = Path(os.getenv('PROCESSED_DATA_PATH', './data/processed'))
        
        # Create processed directory
        self.processed_data_path.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        logger.add(
            "./logs/transform_{time:YYYY-MM-DD}.log",
            rotation="1 day",
            retention="30 days",
            level="INFO"
        )
        
        # Initialize Spark
        self.spark = None

    def create_spark_session(self):
        """Crée la session Spark"""
        try:
            self.spark = SparkSession.builder \
                .appName("CreditCardFraudDetection") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .getOrCreate()
            
            logger.info("✅ Spark session created successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create Spark session: {e}")
            return False

    def load_raw_data(self):
        """Charge les données brutes"""
        try:
            csv_file = self.raw_data_path / "creditcard.csv"
            
            if not csv_file.exists():
                logger.error(f"❌ Raw data file not found: {csv_file}")
                return None
            
            logger.info(f"📊 Loading data from {csv_file}")
            
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(str(csv_file))
            
            logger.info(f"✅ Data loaded: {df.count():,} rows, {len(df.columns)} columns")
            return df
            
        except Exception as e:
            logger.error(f"❌ Failed to load raw data: {e}")
            return None

    def data_quality_checks(self, df):
        """Effectue des contrôles de qualité des données"""
        logger.info("🔍 Performing data quality checks")
        
        # Check for null values
        null_counts = df.select([count(when(isnull(c) | isnan(c), c)).alias(c) for c in df.columns])
        null_report = null_counts.collect()[0].asDict()
        
        total_nulls = sum(null_report.values())
        logger.info(f"📊 Total null values: {total_nulls:,}")
        
        if total_nulls > 0:
            for col_name, null_count in null_report.items():
                if null_count > 0:
                    logger.warning(f"⚠️ Column '{col_name}' has {null_count:,} null values")
        
        # Check class distribution
        class_dist = df.groupBy("Class").count().orderBy("Class").collect()
        logger.info("📊 Class distribution:")
        for row in class_dist:
            percentage = (row['count'] / df.count()) * 100
            logger.info(f"   Class {row['Class']}: {row['count']:,} ({percentage:.2f}%)")
        
        return True

    def feature_engineering(self, df):
        """Crée de nouvelles features"""
        logger.info("🔧 Starting feature engineering")
        
        # Add ID column
        df = df.withColumn("transaction_id", monotonically_increasing_id())
        
        # Add processing timestamp
        df = df.withColumn("processed_at", current_timestamp())
        
        # Amount transformations
        df = df.withColumn("amount_log", when(col("Amount") > 0, 
                                            col("Amount").cast("double").log()).otherwise(0))
        
        # Time transformations (assuming Time is in seconds)
        df = df.withColumn("hour_of_day", (col("Time") % 86400) / 3600)
        df = df.withColumn("day_of_dataset", (col("Time") / 86400).cast("int"))
        
        # Amount categories
        df = df.withColumn("amount_category",
                          when(col("Amount") == 0, "zero")
                          .when(col("Amount") <= 50, "low")
                          .when(col("Amount") <= 200, "medium")
                          .when(col("Amount") <= 1000, "high")
                          .otherwise("very_high"))
        
        # Statistical features for Amount
        amount_stats = df.select(
            mean("Amount").alias("amount_mean"),
            stddev("Amount").alias("amount_std")
        ).collect()[0]
        
        df = df.withColumn("amount_zscore", 
                          (col("Amount") - lit(amount_stats.amount_mean)) / 
                          lit(amount_stats.amount_std))
        
        logger.info(f"✅ Feature engineering completed. New columns: {len(df.columns)}")
        return df

    def data_cleaning(self, df):
        """Nettoie les données"""
        logger.info("🧹 Starting data cleaning")
        
        initial_count = df.count()
        
        # Remove any potential duplicates (based on all original columns)
        original_cols = [col for col in df.columns if col not in 
                        ["transaction_id", "processed_at", "amount_log", "hour_of_day", 
                         "day_of_dataset", "amount_category", "amount_zscore"]]
        
        df_clean = df.dropDuplicates(original_cols)
        
        # Handle any remaining null values (if any)
        df_clean = df_clean.na.drop()
        
        final_count = df_clean.count()
        removed_count = initial_count - final_count
        
        logger.info(f"🧹 Cleaning completed: removed {removed_count:,} rows ({removed_count/initial_count*100:.2f}%)")
        logger.info(f"📊 Final dataset: {final_count:,} rows")
        
        return df_clean

    def save_processed_data(self, df):
        """Sauvegarde les données transformées"""
        try:
            output_path = self.processed_data_path / "credit_card_processed"
            
            logger.info(f"💾 Saving processed data to {output_path}")
            
            # Save as Parquet with partitioning by Class for performance
            df.write \
                .mode("overwrite") \
                .partitionBy("Class") \
                .parquet(str(output_path))
            
            # Also save as single CSV for easy inspection
            df.coalesce(1).write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(str(self.processed_data_path / "credit_card_processed_csv"))
            
            logger.info("✅ Processed data saved successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to save processed data: {e}")
            return False

    def generate_summary_stats(self, df):
        """Génère des statistiques de résumé"""
        logger.info("📊 Generating summary statistics")
        
        try:
            # Basic statistics
            stats_df = df.describe()
            stats_path = self.processed_data_path / "summary_statistics"
            
            stats_df.write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(str(stats_path))
            
            # Fraud statistics by hour
            fraud_by_hour = df.groupBy("hour_of_day", "Class") \
                .count() \
                .orderBy("hour_of_day", "Class")
            
            fraud_stats_path = self.processed_data_path / "fraud_by_hour"
            fraud_by_hour.write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(str(fraud_stats_path))
            
            logger.info("✅ Summary statistics generated")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to generate summary statistics: {e}")
            return False

    def run_transformation(self):
        """Execute le processus de transformation complet"""
        logger.info("🚀 Starting PySpark transformation")
        
        try:
            # Create Spark session
            if not self.create_spark_session():
                return False
            
            # Load raw data
            df = self.load_raw_data()
            if df is None:
                return False
            
            # Data quality checks
            self.data_quality_checks(df)
            
            # Feature engineering
            df_engineered = self.feature_engineering(df)
            
            # Data cleaning
            df_clean = self.data_cleaning(df_engineered)
            
            # Save processed data
            if not self.save_processed_data(df_clean):
                return False
            
            # Generate summary statistics
            if not self.generate_summary_stats(df_clean):
                return False
            
            logger.info("✅ Transformation completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"💥 Transformation failed: {e}")
            return False
            
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("🛑 Spark session stopped")

def main():
    """Point d'entrée principal"""
    transformer = SparkTransformer()
    
    try:
        success = transformer.run_transformation()
        if success:
            logger.info("🎉 Transformation process completed successfully")
        else:
            logger.error("💥 Transformation process failed")
            exit(1)
            
    except Exception as e:
        logger.error(f"💥 Unexpected error during transformation: {e}")
        exit(1)

if __name__ == "__main__":
    main()
