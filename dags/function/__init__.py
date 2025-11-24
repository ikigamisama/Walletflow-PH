import subprocess
import psycopg2
from psycopg2 import sql

from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import TopicAlreadyExistsError, KafkaError


def check_kafka_health():
    print("🔍 Checking Kafka/Redpanda connectivity...")

    try:
        # Try creating a producer connection
        producer = KafkaProducer(
            bootstrap_servers=['redpanda:9092'],
            request_timeout_ms=10000,
            connections_max_idle_ms=30000
        )
        producer.close()

        # Query metadata using admin client
        admin = KafkaAdminClient(bootstrap_servers=['redpanda:9092'])
        topics = admin.list_topics()

        print(f"✅ Kafka is healthy. Found {len(topics)} topics.")
        admin.close()
        return True

    except KafkaError as e:
        print(f"❌ Kafka connection failed: {e}")
        raise Exception(f"Kafka health check failed: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        raise


def create_kafka_topic_func():
    print("📝 Creating Kafka topic: walletflow-bronze-transactions")

    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=['redpanda:9092'],
            request_timeout_ms=10000
        )

        topic_name = "walletflow-bronze-transactions"

        topic = NewTopic(
            name=topic_name,
            num_partitions=6,
            replication_factor=1,
            topic_configs={
                'retention.ms': '604800000',      # 7 days
                'compression.type': 'gzip',
                'max.message.bytes': '1048576'    # 1MB
            }
        )

        try:
            admin_client.create_topics([topic], validate_only=False)
            print("✅ Topic created successfully")
        except TopicAlreadyExistsError:
            print("ℹ️ Topic already exists (OK)")

        # ---- VERIFY TOPIC EXISTS ----
        consumer = KafkaConsumer(bootstrap_servers=['redpanda:9092'])
        topics = consumer.topics()
        consumer.close()

        if topic_name in topics:
            print("✅ Topic verified and ready")
        else:
            raise Exception("❌ Topic creation verification failed")

        admin_client.close()
        return True

    except KafkaError as e:
        print(f"❌ Kafka admin error: {e}")
        raise
    except Exception as e:
        print(f"❌ Topic creation failed: {e}")
        raise


def verify_kafka_messages():
    """Verify messages were successfully written to Kafka"""

    try:
        consumer = KafkaConsumer(
            'walletflow-bronze-transactions',
            bootstrap_servers=['redpanda:9092'],
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000,
            group_id='airflow-verification'
        )

        # Count messages
        message_count = 0
        for message in consumer:
            message_count += 1
            if message_count >= 100:  # Sample first 100
                break

        consumer.close()

        if message_count > 0:
            print(
                f"✅ Verification passed: Found {message_count}+ messages in topic")
            return True
        else:
            print("❌ Verification failed: No messages found in topic")
            raise Exception("No messages in Kafka topic")

    except Exception as e:
        print(f"❌ Kafka verification error: {e}")
        raise


def check_kafka_connectivity():

    kafka_broker = "redpanda:9092"
    topic = "walletflow-bronze-transactions"

    print(f"🔌 Checking Kafka broker: {kafka_broker}")

    try:
        admin = KafkaAdminClient(
            bootstrap_servers=kafka_broker,
            request_timeout_ms=20000,
        )

        topics = admin.list_topics()

        if topic not in topics:
            raise Exception(f"Topic '{topic}' does not exist in Kafka")

        print(f"✅ Kafka OK — topic '{topic}' exists")

    except Exception as e:
        print(f"❌ Kafka health check failed: {e}")
        raise


def check_postgres_health():
    """
    Validate PostgreSQL is up and responding.
    """

    try:
        conn = psycopg2.connect(
            host="postgres-pipeline",
            database="walletflow",
            user="pipeline_user",
            password="pipeline_password",
            port=5432
        )
        cursor = conn.cursor()
        cursor.execute("SELECT NOW();")
        now = cursor.fetchone()
        print(f"🗄 PostgreSQL OK — server time: {now}")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ PostgreSQL connection failed: {e}")
        raise


def validate_silver_schema():
    """
    Ensure required tables exist before Spark streaming starts.
    """

    required_tables = [
        "silver.fact_transactions",
        "silver.dim_users",
        "silver.dim_merchants",
    ]

    try:
        conn = psycopg2.connect(
            host="postgres-pipeline",
            database="walletflow",
            user="pipeline_user",
            password="pipeline_password",
            port=5432
        )
        cursor = conn.cursor()

        for table in required_tables:
            schema, table_name = table.split(".")
            cursor.execute(
                sql.SQL("""
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                """),
                (schema, table_name)
            )
            exists = cursor.fetchone()[0]

            if exists == 0:
                raise Exception(f"❌ Required table missing: {table}")

            print(f"✔ Table exists: {table}")

        cursor.close()
        conn.close()

        print("🎯 All required Silver tables exist.")

    except Exception as e:
        print(f"❌ Silver schema validation failed: {e}")
        raise


def check_spark_streaming_health():

    print("🔍 Checking Spark Streaming job health...")

    try:
        conn = psycopg2.connect(
            host="postgres-pipeline",
            database="walletflow",
            user="pipeline_user",
            password="pipeline_password",
            port=5432
        )
        cursor = conn.cursor()

        # Check for recent transactions
        cursor.execute("""
            SELECT 
                COUNT(*) as tx_count,
                MAX(created_at) as last_tx
            FROM silver.fact_transactions 
            WHERE created_at >= NOW() - INTERVAL '10 minutes'
        """)

        result = cursor.fetchone()
        tx_count, last_tx = result

        cursor.close()
        conn.close()

        if tx_count and tx_count > 0:
            print(
                f"✅ Streaming is healthy: {tx_count} transactions in last 10 min")
            print(f"   Last transaction: {last_tx}")
            return True
        else:
            print(
                "⚠️  No recent transactions (streaming may be down or generator not run)")
            return True  # Don't fail

    except Exception as e:
        print(f"❌ Health check failed: {e}")
        return True  # Don't fail DAG
