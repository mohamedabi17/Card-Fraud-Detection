#!/usr/bin/env python3
"""
Credit Card Fraud Detection - Data Ingestion Script
Télécharge et prépare les données depuis Kaggle
"""

import os
import zipfile
import shutil
from pathlib import Path
from datetime import datetime
from loguru import logger
from dotenv import load_dotenv
import kaggle

# Load environment variables
load_dotenv()

class DataIngester:
    def __init__(self):
        self.raw_data_path = Path(os.getenv('RAW_DATA_PATH', './data/raw'))
        self.archive_path = Path(os.getenv('ARCHIVE_DATA_PATH', './data/archive'))
        self.dataset_name = "mlg-ulb/creditcardfraud"
        
        # Create directories if they don't exist
        self.raw_data_path.mkdir(parents=True, exist_ok=True)
        self.archive_path.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        logger.add(
            "./logs/ingest_{time:YYYY-MM-DD}.log",
            rotation="1 day",
            retention="30 days",
            level="INFO"
        )

    def check_kaggle_config(self):
        """Vérifie la configuration Kaggle API"""
        try:
            kaggle.api.authenticate()
            logger.info("✅ Kaggle API authentication successful")
            return True
        except Exception as e:
            logger.error(f"❌ Kaggle API authentication failed: {e}")
            logger.info("💡 Please ensure ~/.kaggle/kaggle.json exists with valid credentials")
            return False

    def download_dataset(self):
        """Télécharge le dataset depuis Kaggle"""
        try:
            logger.info(f"📥 Starting download of {self.dataset_name}")
            
            # Download to raw data path
            kaggle.api.dataset_download_files(
                self.dataset_name,
                path=str(self.raw_data_path),
                unzip=True
            )
            
            logger.info(f"✅ Dataset downloaded to {self.raw_data_path}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to download dataset: {e}")
            return False

    def validate_data(self):
        """Valide les données téléchargées"""
        csv_file = self.raw_data_path / "creditcard.csv"
        
        if not csv_file.exists():
            logger.error(f"❌ Expected file not found: {csv_file}")
            return False
        
        file_size = csv_file.stat().st_size / (1024 * 1024)  # MB
        logger.info(f"📊 Dataset size: {file_size:.2f} MB")
        
        if file_size < 100:  # Expected size > 100MB
            logger.warning(f"⚠️ Dataset size seems small: {file_size:.2f} MB")
        
        return True

    def archive_previous_data(self):
        """Archive les données précédentes si elles existent"""
        csv_file = self.raw_data_path / "creditcard.csv"
        
        if csv_file.exists():
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_file = self.archive_path / f"creditcard_{timestamp}.csv"
            
            shutil.move(str(csv_file), str(archive_file))
            logger.info(f"📦 Previous data archived to {archive_file}")

    def run_ingestion(self):
        """Execute le processus d'ingestion complet"""
        logger.info("🚀 Starting Credit Card Fraud data ingestion")
        
        # Check Kaggle configuration
        if not self.check_kaggle_config():
            return False
        
        # Archive previous data
        self.archive_previous_data()
        
        # Download new data
        if not self.download_dataset():
            return False
        
        # Validate downloaded data
        if not self.validate_data():
            return False
        
        logger.info("✅ Data ingestion completed successfully")
        return True

def main():
    """Point d'entrée principal"""
    ingester = DataIngester()
    
    try:
        success = ingester.run_ingestion()
        if success:
            logger.info("🎉 Ingestion process completed successfully")
        else:
            logger.error("💥 Ingestion process failed")
            exit(1)
            
    except Exception as e:
        logger.error(f"💥 Unexpected error during ingestion: {e}")
        exit(1)

if __name__ == "__main__":
    main()
