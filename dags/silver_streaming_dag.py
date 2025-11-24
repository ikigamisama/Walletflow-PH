from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

from function import check_kafka_connectivity, check_postgres_health, validate_silver_schema, check_spark_streaming_health

default_args = {
    'owner': 'ikigami',
}


with DAG(
    '2-walletflow_silver_streaming_manager',
    default_args=default_args,
    start_date=datetime(2025, 11, 1),
    description="Manage Spark Streaming job for Silver layer",
    schedule='@daily',
    catchup=False,
    tags=['silver', 'streaming', 'walletflow'],
) as dag:

    kafka_health = PythonOperator(
        task_id='check_kafka_connectivity',
        python_callable=check_kafka_connectivity
    )

    postgres_health = PythonOperator(
        task_id='check_postgres_health',
        python_callable=check_postgres_health
    )

    schema_validation = PythonOperator(
        task_id='validate_silver_schema',
        python_callable=validate_silver_schema
    )

    # start_streaming = SparkSubmitOperator(
    #     task_id='start_spark_streaming',
    #     application='/opt/airflow/dags/spark-jobs/streaming_bronze_to_silver.py',
    #     conn_id='spark_default',
    #     packages='org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0-preview4,org.postgresql:postgresql:42.6.0',
    #     verbose=True,
    #     conf={
    #         'spark.master': 'local[*]',
    #         'spark.driver.memory': '2g',
    #         'spark.executor.memory': '2g',
    #         'spark.sql.streaming.checkpointLocation': '/tmp/checkpoint',
    #         'spark.jars.ivy': '/tmp/.ivy2',
    #         'spark.sql.streaming.schemaInference': 'true',
    #     },
    #     # Important: Don't let it auto-restart
    #     application_args=[],
    # )

    kafka_health >> postgres_health >> schema_validation
