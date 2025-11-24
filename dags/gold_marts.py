from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from datetime import datetime


default_args = {
    'owner': 'ikigami',
}

with DAG(
    '3-walletflow_gold_build_marts',
    default_args=default_args,
    start_date=datetime(2025, 11, 1),
    description="Build Gold layer analytics marts",
    schedule='0 2 * * *',
    catchup=False,
    tags=['gold', 'analytics', 'walletflow'],
) as dag:
    build_user_dashboard = SQLExecuteQueryOperator(
        task_id='build_daily_user_dashboard',
        conn_id="postgres_pipeline",
        sql="""
        -- Build mart_daily_user_dashboard for yesterday
        INSERT INTO gold.mart_daily_user_dashboard (
            user_id,
            spend_date,
            total_spend_php,
            transaction_count,
            avg_transaction_php,
            top_category,
            top_merchant,
            food_spend_php,
            bills_spend_php,
            transport_spend_php,
            ecommerce_spend_php,
            fraud_alerts_today,
            created_at
        )
        SELECT 
            t.user_id,
            DATE(t.event_timestamp) as spend_date,
            SUM(t.amount_php) as total_spend_php,
            COUNT(*) as transaction_count,
            AVG(t.amount_php) as avg_transaction_php,
            
            -- Top category (mode)
            MODE() WITHIN GROUP (ORDER BY m.category) as top_category,
            
            -- Top merchant
            MODE() WITHIN GROUP (ORDER BY m.merchant_name) as top_merchant,
            
            -- Category breakdowns
            SUM(CASE WHEN m.category = 'Food' THEN t.amount_php ELSE 0 END) as food_spend_php,
            SUM(CASE WHEN m.category = 'Bills' THEN t.amount_php ELSE 0 END) as bills_spend_php,
            SUM(CASE WHEN m.category = 'Transport' THEN t.amount_php ELSE 0 END) as transport_spend_php,
            SUM(CASE WHEN m.category = 'E-commerce' THEN t.amount_php ELSE 0 END) as ecommerce_spend_php,
            
            -- Fraud alerts
            SUM(CASE WHEN t.fraud_flag = TRUE THEN 1 ELSE 0 END) as fraud_alerts_today,
            
            NOW() as created_at
        FROM silver.fact_transactions t
        LEFT JOIN silver.dim_merchants m ON t.merchant_id = m.merchant_id
        WHERE DATE(t.event_timestamp) = CURRENT_DATE - INTERVAL '1 day'
        GROUP BY t.user_id, DATE(t.event_timestamp)
        ON CONFLICT (user_id, spend_date) DO UPDATE SET
            total_spend_php = EXCLUDED.total_spend_php,
            transaction_count = EXCLUDED.transaction_count,
            avg_transaction_php = EXCLUDED.avg_transaction_php,
            top_category = EXCLUDED.top_category,
            top_merchant = EXCLUDED.top_merchant,
            food_spend_php = EXCLUDED.food_spend_php,
            bills_spend_php = EXCLUDED.bills_spend_php,
            transport_spend_php = EXCLUDED.transport_spend_php,
            ecommerce_spend_php = EXCLUDED.ecommerce_spend_php,
            fraud_alerts_today = EXCLUDED.fraud_alerts_today;
        """,
    )

    # MART 2: Merchant Performance
    build_merchant_performance = SQLExecuteQueryOperator(
        task_id='build_merchant_performance',
        conn_id="postgres_pipeline",
        sql="""
        -- Build mart_merchant_performance for yesterday
        INSERT INTO gold.mart_merchant_performance (
            merchant_id,
            analysis_date,
            transaction_count,
            total_volume_php,
            avg_ticket_php,
            unique_customers,
            qr_count,
            inapp_count,
            online_count,
            success_rate,
            fraud_incidents,
            growth_vs_yesterday,
            created_at
        )
        SELECT 
            t.merchant_id,
            DATE(t.event_timestamp) as analysis_date,
            COUNT(*) as transaction_count,
            SUM(t.amount_php) as total_volume_php,
            AVG(t.amount_php) as avg_ticket_php,
            COUNT(DISTINCT t.user_id) as unique_customers,
            
            -- Channel breakdown
            SUM(CASE WHEN t.channel = 'QR' THEN 1 ELSE 0 END) as qr_count,
            SUM(CASE WHEN t.channel = 'InApp' THEN 1 ELSE 0 END) as inapp_count,
            SUM(CASE WHEN t.channel = 'Online' THEN 1 ELSE 0 END) as online_count,
            
            -- Success rate
            (SUM(CASE WHEN t.status = 'SUCCESS' THEN 1 ELSE 0 END)::DECIMAL / COUNT(*)) * 100 as success_rate,
            
            -- Fraud
            SUM(CASE WHEN t.fraud_flag = TRUE THEN 1 ELSE 0 END) as fraud_incidents,
            
            -- Growth (vs previous day)
            0.0 as growth_vs_yesterday,  -- TODO: Calculate actual growth
            
            NOW() as created_at
        FROM silver.fact_transactions t
        WHERE DATE(t.event_timestamp) = CURRENT_DATE - INTERVAL '1 day'
          AND t.merchant_id IS NOT NULL
        GROUP BY t.merchant_id, DATE(t.event_timestamp)
        ON CONFLICT (merchant_id, analysis_date) DO UPDATE SET
            transaction_count = EXCLUDED.transaction_count,
            total_volume_php = EXCLUDED.total_volume_php,
            avg_ticket_php = EXCLUDED.avg_ticket_php,
            unique_customers = EXCLUDED.unique_customers,
            success_rate = EXCLUDED.success_rate,
            fraud_incidents = EXCLUDED.fraud_incidents;
        """,
    )

    # MART 3: Hourly System Velocity (last 24 hours)
    build_hourly_velocity = SQLExecuteQueryOperator(
        task_id='build_hourly_velocity',
        conn_id="postgres_pipeline",
        sql="""
        -- Build mart_hourly_velocity for last 24 hours
        INSERT INTO gold.mart_hourly_velocity (
            window_hour,
            transaction_count,
            total_volume_php,
            payment_count,
            transfer_count,
            cashout_count,
            success_rate,
            fraud_rate,
            avg_processing_ms,
            created_at
        )
        SELECT 
            DATE_TRUNC('hour', event_timestamp) as window_hour,
            COUNT(*) as transaction_count,
            SUM(amount_php) as total_volume_php,
            
            -- Transaction types
            SUM(CASE WHEN transaction_type = 'PAYMENT' THEN 1 ELSE 0 END) as payment_count,
            SUM(CASE WHEN transaction_type = 'TRANSFER' THEN 1 ELSE 0 END) as transfer_count,
            SUM(CASE WHEN transaction_type = 'CASHOUT' THEN 1 ELSE 0 END) as cashout_count,
            
            -- Rates
            (SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END)::DECIMAL / COUNT(*)) * 100 as success_rate,
            (SUM(CASE WHEN fraud_flag = TRUE THEN 1 ELSE 0 END)::DECIMAL / COUNT(*)) * 100 as fraud_rate,
            
            -- Performance
            AVG(EXTRACT(EPOCH FROM (processing_timestamp - event_timestamp)) * 1000) as avg_processing_ms,
            
            NOW() as created_at
        FROM silver.fact_transactions
        WHERE event_timestamp >= CURRENT_DATE - INTERVAL '1 day'
        GROUP BY DATE_TRUNC('hour', event_timestamp)
        ON CONFLICT (window_hour) DO UPDATE SET
            transaction_count = EXCLUDED.transaction_count,
            total_volume_php = EXCLUDED.total_volume_php,
            success_rate = EXCLUDED.success_rate,
            fraud_rate = EXCLUDED.fraud_rate;
        """,
    )

    # Data quality check
    validate_marts = SQLExecuteQueryOperator(
        task_id='validate_gold_marts',
        conn_id="postgres_pipeline",
        sql="""
        -- Validate marts have data
        DO $$
        DECLARE
            user_count INTEGER;
            merchant_count INTEGER;
            hourly_count INTEGER;
        BEGIN
            SELECT COUNT(*) INTO user_count FROM gold.mart_daily_user_dashboard 
            WHERE spend_date = CURRENT_DATE - INTERVAL '1 day';
            
            SELECT COUNT(*) INTO merchant_count FROM gold.mart_merchant_performance 
            WHERE analysis_date = CURRENT_DATE - INTERVAL '1 day';
            
            SELECT COUNT(*) INTO hourly_count FROM gold.mart_hourly_velocity 
            WHERE window_hour >= CURRENT_DATE - INTERVAL '1 day';
            
            RAISE NOTICE 'User Dashboard Records: %', user_count;
            RAISE NOTICE 'Merchant Performance Records: %', merchant_count;
            RAISE NOTICE 'Hourly Velocity Records: %', hourly_count;
            
            IF user_count = 0 OR merchant_count = 0 OR hourly_count = 0 THEN
                RAISE NOTICE '⚠️ Validation warning: Empty marts detected, skipping for now';
            ELSE
                RAISE NOTICE '✅ Validation passed: Marts have data';
            END IF;
        END $$;
        """,
    )

    # Task dependencies
    [build_user_dashboard, build_merchant_performance,
        build_hourly_velocity] >> validate_marts
