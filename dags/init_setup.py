from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from function import check_spark_streaming_health

from datetime import datetime

default_args = {
    'owner': 'ikigami',
}

with DAG(
    '0-walletflow_init_setup',
    default_args=default_args,
    start_date=datetime(2025, 11, 1),
    description="Initialize Database layer schema and monitor streaming",
    schedule='@once',
    catchup=False,
    tags=['init', 'setup', 'walletflow'],
) as dag:
    create_silver_schema = SQLExecuteQueryOperator(
        task_id='create_silver_schema',
        conn_id="postgres_pipeline",
        sql="""
        -- Create schemas
        CREATE SCHEMA IF NOT EXISTS silver;
        CREATE SCHEMA IF NOT EXISTS gold;

        -- Silver Layer: dim_users
        CREATE TABLE IF NOT EXISTS silver.dim_users (
            user_id VARCHAR(50) PRIMARY KEY,
            phone_hash VARCHAR(64),
            full_name VARCHAR(150),
            home_province VARCHAR(50),
            home_city VARCHAR(50),
            kyc_level VARCHAR(20) DEFAULT 'Level1',
            account_status VARCHAR(20) DEFAULT 'Active',
            registration_date DATE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_users_province ON silver.dim_users(home_province);

        -- Silver Layer: dim_merchants
        CREATE TABLE IF NOT EXISTS silver.dim_merchants (
            merchant_id VARCHAR(50) PRIMARY KEY,
            merchant_name VARCHAR(200),
            category VARCHAR(50),
            subcategory VARCHAR(50),
            province VARCHAR(50),
            city VARCHAR(100),
            is_partner BOOLEAN DEFAULT FALSE,
            onboarding_date DATE,
            risk_score SMALLINT DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_merchants_category ON silver.dim_merchants(category);
        CREATE INDEX IF NOT EXISTS idx_merchants_province ON silver.dim_merchants(province);

        -- Silver Layer: fact_transactions (Main event table)
        CREATE TABLE IF NOT EXISTS silver.fact_transactions (
            transaction_id VARCHAR(100) PRIMARY KEY,
            event_timestamp TIMESTAMPTZ NOT NULL,
            user_id VARCHAR(50) REFERENCES silver.dim_users(user_id),
            merchant_id VARCHAR(50) REFERENCES silver.dim_merchants(merchant_id),
            transaction_type VARCHAR(20) NOT NULL,
            amount_php DECIMAL(14,2) NOT NULL CHECK (amount_php > 0),
            channel VARCHAR(20),
            province VARCHAR(50),
            city VARCHAR(100),
            is_international BOOLEAN DEFAULT FALSE,
            device_id VARCHAR(64),
            ip_hash VARCHAR(64),
            fraud_score DECIMAL(5,2) DEFAULT 0,
            fraud_flag BOOLEAN DEFAULT FALSE,
            fraud_reason TEXT,
            status VARCHAR(20) DEFAULT 'SUCCESS',
            processing_timestamp TIMESTAMPTZ DEFAULT NOW(),
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        -- Indexes for performance
        CREATE INDEX IF NOT EXISTS idx_tx_timestamp ON silver.fact_transactions(event_timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_tx_user ON silver.fact_transactions(user_id, event_timestamp);
        CREATE INDEX IF NOT EXISTS idx_tx_merchant ON silver.fact_transactions(merchant_id, event_timestamp);
        CREATE INDEX IF NOT EXISTS idx_tx_fraud ON silver.fact_transactions(fraud_flag) WHERE fraud_flag = TRUE;
        CREATE INDEX IF NOT EXISTS idx_tx_created ON silver.fact_transactions(created_at DESC);
        """,
    )

    create_gold_schema = SQLExecuteQueryOperator(
        task_id='create_gold_schema',
        conn_id="postgres_pipeline",
        sql="""
        -- Gold Layer: mart_daily_user_dashboard
        CREATE TABLE IF NOT EXISTS gold.mart_daily_user_dashboard (
            user_id VARCHAR(50),
            spend_date DATE,
            total_spend_php DECIMAL(16,2),
            transaction_count INT,
            avg_transaction_php DECIMAL(12,2),
            top_category VARCHAR(50),
            top_merchant VARCHAR(200),
            food_spend_php DECIMAL(12,2) DEFAULT 0,
            bills_spend_php DECIMAL(12,2) DEFAULT 0,
            transport_spend_php DECIMAL(12,2) DEFAULT 0,
            ecommerce_spend_php DECIMAL(12,2) DEFAULT 0,
            fraud_alerts_today INT DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (user_id, spend_date)
        );

        CREATE INDEX IF NOT EXISTS idx_user_dashboard_date ON gold.mart_daily_user_dashboard(spend_date);

        -- Gold Layer: mart_fraud_alerts
        CREATE TABLE IF NOT EXISTS gold.mart_fraud_alerts (
            alert_id BIGSERIAL PRIMARY KEY,
            transaction_id VARCHAR(100),
            user_id VARCHAR(50),
            merchant_id VARCHAR(50),
            alert_type VARCHAR(100),
            fraud_score DECIMAL(5,2),
            amount_php DECIMAL(14,2),
            province VARCHAR(50),
            event_timestamp TIMESTAMPTZ,
            status VARCHAR(20) DEFAULT 'NEW',
            investigated_by VARCHAR(100),
            notes TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            resolved_at TIMESTAMPTZ
        );

        CREATE INDEX IF NOT EXISTS idx_alerts_status ON gold.mart_fraud_alerts(status) WHERE status = 'NEW';
        CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON gold.mart_fraud_alerts(event_timestamp DESC);

        -- Gold Layer: mart_merchant_performance
        CREATE TABLE IF NOT EXISTS gold.mart_merchant_performance (
            merchant_id VARCHAR(50),
            analysis_date DATE,
            transaction_count INT,
            total_volume_php DECIMAL(18,2),
            avg_ticket_php DECIMAL(12,2),
            unique_customers INT,
            qr_count INT,
            inapp_count INT,
            online_count INT,
            success_rate DECIMAL(5,2),
            fraud_incidents INT,
            growth_vs_yesterday DECIMAL(6,2),
            created_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (merchant_id, analysis_date)
        );

        -- Gold Layer: mart_hourly_velocity
        CREATE TABLE IF NOT EXISTS gold.mart_hourly_velocity (
            window_hour TIMESTAMPTZ PRIMARY KEY,
            transaction_count INT,
            total_volume_php DECIMAL(16,2),
            payment_count INT,
            transfer_count INT,
            cashout_count INT,
            success_rate DECIMAL(5,2),
            fraud_rate DECIMAL(5,4),
            avg_processing_ms DECIMAL(8,2),
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_hourly_velocity_time ON gold.mart_hourly_velocity(window_hour DESC);
        """
    )

    verify_schema = SQLExecuteQueryOperator(
        task_id='verify_schema',
        conn_id="postgres_pipeline",
        sql="""
        DO $$
        DECLARE
            silver_tables INT;
            gold_tables INT;
        BEGIN
            -- Count Silver tables
            SELECT COUNT(*) INTO silver_tables
            FROM information_schema.tables
            WHERE table_schema = 'silver'
            AND table_type = 'BASE TABLE';

            -- Count Gold tables
            SELECT COUNT(*) INTO gold_tables
            FROM information_schema.tables
            WHERE table_schema = 'gold'
            AND table_type = 'BASE TABLE';

            RAISE NOTICE '✅ Silver Layer: % tables created', silver_tables;
            RAISE NOTICE '✅ Gold Layer: % tables created', gold_tables;

            IF silver_tables < 3 OR gold_tables < 4 THEN
                RAISE EXCEPTION 'Schema validation failed: Missing tables';
            END IF;

            RAISE NOTICE '✅ Schema validation passed!';
        END $$;
        """
    )

    check_health = PythonOperator(
        task_id='check_streaming_health',
        python_callable=check_spark_streaming_health,
    )

    create_silver_schema >> create_gold_schema >> verify_schema >> check_health
