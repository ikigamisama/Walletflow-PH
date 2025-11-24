from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import random

from function import verify_kafka_messages, check_kafka_health, create_kafka_topic_func


default_args = {
    'owner': 'ikigami',
}

with DAG(
    '1-walletflow_bronze_ingestion',
    default_args=default_args,
    start_date=datetime(2025, 11, 1),
    description="Generate synthetic transactions and push to Kafka/Redpanda",
    schedule='0 * * * *',
    catchup=False,
    tags=['bronze', 'ingestion', 'walletflow']
) as dag:

    health_check = PythonOperator(
        task_id='kafka_health_check',
        python_callable=check_kafka_health,
    )

    create_topic = PythonOperator(
        task_id='create_kafka_topic',
        python_callable=create_kafka_topic_func,
    )

    run_generator = BashOperator(
        task_id='run_transaction_generator',
        bash_command=f"""
        echo "🚀 Starting Transaction Generator for 5 Minute..."
        
        python /opt/airflow/dags/generator/transaction_generator.py \
            --duration 3600 \
            --rate 50000 \
            --kafka redpanda:9092 \
            --topic walletflow-bronze-transactions
        
        echo "✅ Generator completed"
        """,
    )

    verify_ingestion = PythonOperator(
        task_id='verify_ingestion',
        python_callable=verify_kafka_messages,
    )

    log_stats = BashOperator(
        task_id='log_statistics',
        bash_command="""
        echo "📊 BRONZE LAYER INGESTION STATISTICS"
        echo "======================================="
        
        # Get topic info
        rpk topic describe walletflow-bronze-transactions --brokers redpanda:9092
        
        echo ""
        echo "✅ Bronze ingestion DAG completed successfully"
        """,
    )

    health_check >> create_topic >> run_generator >> verify_ingestion >> log_stats
