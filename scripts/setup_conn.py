import json
import subprocess
import os

# MinIO connection details from env
minio_conn_id = "aws_default"
minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioLocalAccessKey")
minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioLocalSecretKey123")
minio_endpoint_url = os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000")
minio_region = os.getenv("MINIO_REGION", "us-east-1")

minio_extra = {
    "aws_access_key_id": minio_access_key,
    "aws_secret_access_key": minio_secret_key,
    "region_name": minio_region,
    "endpoint_url": minio_endpoint_url,
}
minio_extra_json = json.dumps(minio_extra)

minio_command = [
    "airflow",
    "connections",
    "add",
    minio_conn_id,
    "--conn-type", "aws",
    "--conn-extra", minio_extra_json,
]

# Postgres connection details from env
pg_conn_id = "postgres_default"
pg_host = os.getenv("POSTGRES_HOST", "postgres")
pg_login = os.getenv("POSTGRES_USER", "airflow")
pg_password = os.getenv("POSTGRES_PASSWORD", "airflow")
pg_schema = os.getenv("POSTGRES_DB", "airflow")
pg_port = os.getenv("POSTGRES_PORT", "5432")

pg_command = [
    "airflow",
    "connections",
    "add",
    pg_conn_id,
    "--conn-type", "postgres",
    "--conn-host", pg_host,
    "--conn-login", pg_login,
    "--conn-password", pg_password,
    "--conn-schema", pg_schema,
    "--conn-port", pg_port,
]

pipeline_pg_conn_id = "postgres_pipeline"
pipeline_pg_host = os.getenv("POSTGRES_PIPELINE_HOST", "postgres-pipeline")
pipeline_pg_login = os.getenv("POSTGRES_PIPELINE_USER", "pipeline_user")
pipeline_pg_password = os.getenv(
    "POSTGRES_PIPELINE_PASSWORD", "pipeline_password")
pipeline_pg_schema = os.getenv("POSTGRES_PIPELINE_DB", "walletflow")
pipeline_pg_port = os.getenv("POSTGRES_PIPELINE_PORT", "5432")


pipeline_pg_command = [
    "airflow",
    "connections",
    "add",
    pipeline_pg_conn_id,
    "--conn-type", "postgres",
    "--conn-host", pipeline_pg_host,
    "--conn-login", pipeline_pg_login,
    "--conn-password", pipeline_pg_password,
    "--conn-schema", pipeline_pg_schema,
    "--conn-port", pipeline_pg_port,
]

spark_conn_id = "spark_default"
spark_master_host = os.getenv("SPARK_HOST", "spark-streaming")
spark_master_port = os.getenv("SPARK_PORT", "7077")

spark_command = [
    "airflow",
    "connections",
    "add",
    spark_conn_id,
    "--conn-type", "spark",
    "--conn-host", spark_master_host,
    "--conn-port", spark_master_port,
]


def add_connection(command, conn_id):
    try:
        print(f"Adding connection '{conn_id}' to Airflow...")
        result = subprocess.run(command, check=True,
                                capture_output=True, text=True)
        print("✅ Connection added successfully!")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        if "already exists" in (e.stderr or ""):
            print(
                f"⚠️ Connection '{conn_id}' already exists. Deleting and recreating...")
            subprocess.run(["airflow", "connections",
                           "delete", conn_id], check=True)
            subprocess.run(command, check=True)
            print("✅ Connection updated successfully!")
        else:
            print(f"❌ Error adding connection '{conn_id}': {e.stderr}")
            raise


if __name__ == "__main__":
    add_connection(minio_command, minio_conn_id)
    add_connection(pg_command, pg_conn_id)
    add_connection(pipeline_pg_command, pipeline_pg_conn_id)
    add_connection(spark_command, spark_conn_id)
