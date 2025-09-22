-- Initial database setup for Credit Card Fraud Detection
-- This script runs automatically when PostgreSQL container starts

\echo 'Setting up Credit Card Fraud Detection database...'

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Create indexes for performance (will be created by load script but good to have as backup)
\echo 'Database setup completed successfully!'
