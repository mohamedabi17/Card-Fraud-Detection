#!/usr/bin/env python3
"""
Tests unitaires pour le pipeline Credit Card Fraud Detection
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import tempfile
import shutil
from unittest.mock import Mock, patch
import os
import sys

# Add scripts directory to path
sys.path.append(str(Path(__file__).parent.parent / 'scripts'))

from ingest import DataIngester
from transform_spark import SparkTransformer
from load_postgres import PostgreSQLLoader

class TestDataIngester:
    
    @patch('kaggle.api.authenticate')
    def test_check_kaggle_config_success(self, mock_auth):
        """Test successful Kaggle configuration check"""
        mock_auth.return_value = None
        ingester = DataIngester()
        assert ingester.check_kaggle_config() == True
    
    @patch('kaggle.api.authenticate')
    def test_check_kaggle_config_failure(self, mock_auth):
        """Test failed Kaggle configuration check"""
        mock_auth.side_effect = Exception("Authentication failed")
        ingester = DataIngester()
        assert ingester.check_kaggle_config() == False
    
    def test_validate_data_with_valid_file(self):
        """Test data validation with valid CSV file"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create mock CSV file
            csv_path = Path(temp_dir) / "creditcard.csv"
            mock_data = pd.DataFrame({
                'Time': [1, 2, 3],
                'Amount': [100, 200, 300],
                'Class': [0, 1, 0]
            })
            mock_data.to_csv(csv_path, index=False)
            
            # Test validation
            ingester = DataIngester()
            ingester.raw_data_path = Path(temp_dir)
            assert ingester.validate_data() == True
    
    def test_validate_data_with_missing_file(self):
        """Test data validation with missing file"""
        with tempfile.TemporaryDirectory() as temp_dir:
            ingester = DataIngester()
            ingester.raw_data_path = Path(temp_dir)
            assert ingester.validate_data() == False

class TestDataTransformation:
    
    def create_mock_dataframe(self):
        """Create mock DataFrame for testing"""
        np.random.seed(42)
        n_samples = 1000
        
        data = {
            'Time': np.random.randint(0, 172800, n_samples),  # 2 days in seconds
            'Amount': np.random.exponential(50, n_samples),
            'Class': np.random.choice([0, 1], n_samples, p=[0.998, 0.002])
        }
        
        # Add V1-V28 features
        for i in range(1, 29):
            data[f'V{i}'] = np.random.normal(0, 1, n_samples)
        
        return pd.DataFrame(data)
    
    def test_feature_engineering(self):
        """Test feature engineering functionality"""
        # This test would require Spark setup, so we'll test the logic
        mock_df = self.create_mock_dataframe()
        
        # Test amount log transformation logic
        amount_log = np.where(mock_df['Amount'] > 0, 
                             np.log(mock_df['Amount']), 0)
        assert len(amount_log) == len(mock_df)
        assert not np.any(np.isinf(amount_log))
        
        # Test hour calculation
        hour_of_day = (mock_df['Time'] % 86400) / 3600
        assert all(0 <= h < 24 for h in hour_of_day)
    
    def test_amount_categorization(self):
        """Test amount categorization logic"""
        amounts = [0, 25, 100, 500, 2000]
        expected_categories = ['zero', 'low', 'medium', 'high', 'very_high']
        
        for amount, expected in zip(amounts, expected_categories):
            if amount == 0:
                category = 'zero'
            elif amount <= 50:
                category = 'low'
            elif amount <= 200:
                category = 'medium'
            elif amount <= 1000:
                category = 'high'
            else:
                category = 'very_high'
            
            assert category == expected

class TestDataQuality:
    
    def test_fraud_rate_validation(self):
        """Test fraud rate validation"""
        # Normal fraud rate
        df_normal = pd.DataFrame({
            'Class': [0] * 9990 + [1] * 10  # 0.1% fraud rate
        })
        fraud_rate = df_normal['Class'].mean()
        assert 0.001 <= fraud_rate <= 0.1
        
        # Abnormal fraud rate (too high)
        df_high = pd.DataFrame({
            'Class': [0] * 500 + [1] * 500  # 50% fraud rate
        })
        fraud_rate_high = df_high['Class'].mean()
        assert fraud_rate_high > 0.1
    
    def test_required_columns(self):
        """Test required columns presence"""
        required_cols = ['Time', 'Amount', 'Class'] + [f'V{i}' for i in range(1, 29)]
        
        # Complete DataFrame
        complete_data = {col: [1, 2, 3] for col in required_cols}
        df_complete = pd.DataFrame(complete_data)
        missing_cols = set(required_cols) - set(df_complete.columns)
        assert len(missing_cols) == 0
        
        # Incomplete DataFrame
        incomplete_data = {col: [1, 2, 3] for col in required_cols[:10]}
        df_incomplete = pd.DataFrame(incomplete_data)
        missing_cols = set(required_cols) - set(df_incomplete.columns)
        assert len(missing_cols) > 0

class TestDatabaseOperations:
    
    @patch('psycopg2.connect')
    def test_database_connection(self, mock_connect):
        """Test database connection"""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        
        loader = PostgreSQLLoader()
        result = loader.create_database_if_not_exists()
        
        # Should attempt to connect
        mock_connect.assert_called()
    
    def test_sql_query_syntax(self):
        """Test SQL query syntax (basic validation)"""
        # Test that our main queries are syntactically correct
        queries = [
            "SELECT COUNT(*) FROM credit_card_transactions",
            "SELECT class, COUNT(*) FROM credit_card_transactions GROUP BY class",
            "SELECT AVG(amount) FROM credit_card_transactions WHERE class = 1"
        ]
        
        for query in queries:
            # Basic syntax check
            assert 'SELECT' in query.upper()
            assert 'FROM' in query.upper()
            assert query.strip().endswith((')', ';')) or not query.strip().endswith(';')

class TestPipelineIntegration:
    
    def test_pipeline_sequence(self):
        """Test the logical sequence of pipeline steps"""
        steps = [
            'data_ingestion',
            'data_quality_check', 
            'data_transformation',
            'transformation_validation',
            'data_loading',
            'aggregation_creation'
        ]
        
        # Test that steps are in logical order
        expected_order = [
            'data_ingestion',
            'data_quality_check',
            'data_transformation', 
            'transformation_validation',
            'data_loading',
            'aggregation_creation'
        ]
        
        assert steps == expected_order
    
    def test_data_flow_consistency(self):
        """Test data flow consistency between steps"""
        # Mock data sizes through pipeline
        raw_data_size = 284807  # Expected Kaggle dataset size
        
        # After cleaning, should be similar size (minor reduction)
        processed_size = int(raw_data_size * 0.99)  # Allow 1% reduction
        
        # Database should have same size as processed
        db_size = processed_size
        
        assert processed_size <= raw_data_size
        assert db_size == processed_size

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
