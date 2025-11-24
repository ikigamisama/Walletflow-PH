from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, lit, when,
    sha2, concat_ws, current_timestamp, window,
    count, sum as _sum, avg, max as _max, lag
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DecimalType, BooleanType, TimestampType
)
from pyspark.sql.window import Window

BRONZE_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_timestamp", StringType(), False),
    StructField("transaction_type", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("phone_number", StringType(), True),
    StructField("user_name", StringType(), True),
    StructField("amount_php", StringType(), False),
    StructField("merchant_id", StringType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("province", StringType(), True),
    StructField("city", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("is_international", BooleanType(), True),
    StructField("device_id", StringType(), True),
    StructField("ip_address", StringType(), True),
])

POSTGRES_CONFIG = {
    "url": "jdbc:postgresql://postgres-pipeline:5432/walletflow",
    "driver": "org.postgresql.Driver",
    "user": "pipeline_user",
    "password": "pipeline_password",
    "properties": {
        "stringtype": "unspecified"
    }
}


class SilverStreamingJob:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("WalletFlow-Bronze-to-Silver") \
            .config("spark.master", "local[*]") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
            .config(
                "spark.jars.packages",
                "org.postgresql:postgresql:42.6.0,org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0-preview4"
            ) \
            .config("spark.sql.shuffle.partitions", "6") \
            .config("spark.jars.ivy", "/tmp/.ivy2") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")
        print("✅ Spark Session created")

    def read_bronze_stream(self):
        """Read from Kafka Bronze layer"""
        print("📖 Reading from Kafka topic: walletflow-bronze-transactions")

        kafka_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "redpanda:9092") \
            .option("subscribe", "walletflow-bronze-transactions") \
            .option("startingOffsets", "latest") \
            .option("maxOffsetsPerTrigger", 30000) \
            .option("failOnDataLoss", "false") \
            .load()

        kafka_df = kafka_df.filter(col("value").isNotNull())

        # Parse JSON
        parsed_df = kafka_df.select(
            from_json(col("value").cast("string"), BRONZE_SCHEMA).alias("data")
        ).filter(col("data").isNotNull()) \
            .select("data.*")

        print("✅ Kafka stream connected")
        return parsed_df

    def cleanse_and_validate(self, df):
        print("🧹 Cleansing and validating data...")

        cleansed_df = df \
            .withColumn("event_timestamp", to_timestamp(col("event_timestamp"))) \
            .withColumn("amount_php", col("amount_php").cast(DecimalType(14, 2))) \
            .withColumn("processing_timestamp", current_timestamp()) \
            .withColumn("phone_hash", sha2(col("phone_number"), 256)) \
            .withColumn("ip_hash", sha2(col("ip_address"), 256)) \
            .withColumn("status", lit("SUCCESS")) \
            .filter(col("amount_php") > 0) \
            .filter(col("event_timestamp").isNotNull())

        return cleansed_df

    def calculate_fraud_score(self, df):
        """Apply fraud detection rules"""
        print("🔒 Calculating fraud scores...")

        # Add hour for time-based rules
        df = df.withColumn("hour", col(
            "event_timestamp").substr(12, 2).cast("int"))

        # Initialize fraud score
        fraud_df = df.withColumn("fraud_score", lit(0.0))
        fraud_df = fraud_df.withColumn(
            "fraud_reason", lit(None).cast(StringType()))

        # Rule 1: High amount at night (00:00-05:00)
        fraud_df = fraud_df.withColumn(
            "fraud_score",
            when((col("amount_php") > 50000) & (
                col("hour").between(0, 5)), 85.0)
            .otherwise(col("fraud_score"))
        ).withColumn(
            "fraud_reason",
            when((col("amount_php") > 50000) & (
                col("hour").between(0, 5)), "HIGH_AMOUNT_NIGHT")
            .otherwise(col("fraud_reason"))
        )

        fraud_df = fraud_df.withColumn(
            "fraud_score",
            when(
                (col("amount_php") > 10000) &
                (col("amount_php") % 1000 == 0) &
                (col("fraud_score") == 0),
                40.0
            ).otherwise(col("fraud_score"))
        ).withColumn(
            "fraud_reason",
            when(
                (col("amount_php") > 10000) &
                (col("amount_php") % 1000 == 0) &
                (col("fraud_reason").isNull()),
                "ROUND_AMOUNT"
            ).otherwise(col("fraud_reason"))
        )

        fraud_df = fraud_df.withColumn(
            "fraud_score",
            when(
                (col("is_international") == True) &
                (col("amount_php") > 30000) &
                (col("fraud_score") == 0),
                65.0
            ).otherwise(col("fraud_score"))
        ).withColumn(
            "fraud_reason",
            when(
                (col("is_international") == True) &
                (col("amount_php") > 30000) &
                (col("fraud_reason").isNull()),
                "INTERNATIONAL_HIGH_AMOUNT"
            ).otherwise(col("fraud_reason"))
        )

        # Set fraud flag for scores > 60
        fraud_df = fraud_df.withColumn(
            "fraud_flag",
            when(col("fraud_score") >= 60, True).otherwise(False)
        )

        fraud_df = fraud_df.drop("hour")

        return fraud_df

    def table_exists(self, table_name):
        try:
            _ = self.spark.read.format("jdbc") \
                .option("url", POSTGRES_CONFIG["url"]) \
                .option("dbtable", table_name) \
                .option("user", POSTGRES_CONFIG["user"]) \
                .option("password", POSTGRES_CONFIG["password"]) \
                .option("driver", POSTGRES_CONFIG["driver"]) \
                .load().limit(1)
            return True
        except Exception:
            return False

    def write_to_postgres(self, batch_df, table_name):
        """Simple append to PostgreSQL"""
        if batch_df.isEmpty():
            print(f"⏭️  Empty batch, skipping {table_name}")
            return

        print(f"💾 Writing {batch_df.count()} records to {table_name}...")

        try:
            batch_df.write \
                .format("jdbc") \
                .option("url", POSTGRES_CONFIG["url"]) \
                .option("dbtable", table_name) \
                .option("user", POSTGRES_CONFIG["user"]) \
                .option("password", POSTGRES_CONFIG["password"]) \
                .option("driver", POSTGRES_CONFIG["driver"]) \
                .mode("append") \
                .save()

            print(f"✅ Batch written to {table_name}")
        except Exception as e:
            if "duplicate key" in str(e).lower() or "unique constraint" in str(e).lower():
                print(f"⚠️  Skipping duplicates in {table_name}")
            else:
                raise

    def process_dimensions(self, batch_df, batch_id):
        """Upsert dimension tables safely without duplicates"""
        print(f"🔄 Processing batch {batch_id} - Dimensions")

        # --- Users ---
        if self.table_exists("silver.dim_users"):
            existing_users = self.spark.read.format("jdbc") \
                .option("url", POSTGRES_CONFIG["url"]) \
                .option("dbtable", "silver.dim_users") \
                .option("user", POSTGRES_CONFIG["user"]) \
                .option("password", POSTGRES_CONFIG["password"]) \
                .option("driver", POSTGRES_CONFIG["driver"]) \
                .load().select("user_id")

            users_df = batch_df.select(
                "user_id",
                "phone_hash",
                col("user_name").alias("full_name"),
                col("province").alias("home_province"),
                col("city").alias("home_city"),
                lit("Level1").alias("kyc_level"),
                lit("Active").alias("account_status"),
                current_timestamp().alias("registration_date"),
                current_timestamp().alias("created_at"),
                current_timestamp().alias("updated_at")
            ).dropDuplicates(["user_id"]) \
                .join(existing_users, "user_id", "left_anti")  # only new users
        else:
            users_df = batch_df.select(
                "user_id",
                "phone_hash",
                col("user_name").alias("full_name"),
                col("province").alias("home_province"),
                col("city").alias("home_city"),
                lit("Level1").alias("kyc_level"),
                lit("Active").alias("account_status"),
                current_timestamp().alias("registration_date"),
                current_timestamp().alias("created_at"),
                current_timestamp().alias("updated_at")
            ).dropDuplicates(["user_id"])

        if not users_df.isEmpty():
            self.write_to_postgres(users_df, "silver.dim_users")

        # --- Merchants ---
        if self.table_exists("silver.dim_merchants"):
            existing_merchants = self.spark.read.format("jdbc") \
                .option("url", POSTGRES_CONFIG["url"]) \
                .option("dbtable", "silver.dim_merchants") \
                .option("user", POSTGRES_CONFIG["user"]) \
                .option("password", POSTGRES_CONFIG["password"]) \
                .option("driver", POSTGRES_CONFIG["driver"]) \
                .load().select("merchant_id")

            merchants_df = batch_df.filter(col("merchant_id").isNotNull()).select(
                "merchant_id",
                "merchant_name",
                col("merchant_category").alias("category"),
                lit(None).cast(StringType()).alias("subcategory"),
                "province",
                "city",
                lit(False).alias("is_partner"),
                current_timestamp().alias("onboarding_date"),
                lit(0).alias("risk_score"),
                current_timestamp().alias("created_at"),
                current_timestamp().alias("updated_at")
            ).dropDuplicates(["merchant_id"]) \
                .join(existing_merchants, "merchant_id", "left_anti")
        else:
            merchants_df = batch_df.filter(col("merchant_id").isNotNull()).select(
                "merchant_id",
                "merchant_name",
                col("merchant_category").alias("category"),
                lit(None).cast(StringType()).alias("subcategory"),
                "province",
                "city",
                lit(False).alias("is_partner"),
                current_timestamp().alias("onboarding_date"),
                lit(0).alias("risk_score"),
                current_timestamp().alias("created_at"),
                current_timestamp().alias("updated_at")
            ).dropDuplicates(["merchant_id"])

        if not merchants_df.isEmpty():
            self.write_to_postgres(merchants_df, "silver.dim_merchants")

    def process_facts(self, batch_df, batch_id):
        """Write transaction facts"""
        print(f"🔄 Processing batch {batch_id} - Facts")

        if not (self.table_exists("silver.dim_users") and self.table_exists("silver.dim_merchants")):
            print("⏭️ Skipping fact_transactions: Required dimension tables not ready")
            return

        existing_users = self.spark.read.format("jdbc") \
            .option("url", POSTGRES_CONFIG["url"]) \
            .option("dbtable", "silver.dim_users") \
            .option("user", POSTGRES_CONFIG["user"]) \
            .option("password", POSTGRES_CONFIG["password"]) \
            .option("driver", POSTGRES_CONFIG["driver"]) \
            .load().select("user_id")

        existing_merchants = self.spark.read.format("jdbc") \
            .option("url", POSTGRES_CONFIG["url"]) \
            .option("dbtable", "silver.dim_merchants") \
            .option("user", POSTGRES_CONFIG["user"]) \
            .option("password", POSTGRES_CONFIG["password"]) \
            .option("driver", POSTGRES_CONFIG["driver"]) \
            .load().select("merchant_id")

        facts_df = batch_df.join(existing_users, "user_id", "inner") \
            .join(existing_merchants, "merchant_id", "inner") \
            .select(
            col("event_id").alias("transaction_id"),
            "event_timestamp",
            "user_id",
            "merchant_id",
            "transaction_type",
            "amount_php",
            "channel",
            "province",
            "city",
            "is_international",
            "device_id",
            "ip_hash",
            "fraud_score",
            "fraud_flag",
            "fraud_reason",
            "status",
            "processing_timestamp"
        )

        if not facts_df.isEmpty():
            self.write_to_postgres(facts_df, "silver.fact_transactions")

    def process_fraud_alerts(self, batch_df, batch_id):
        """Generate fraud alerts for flagged transactions"""
        print(f"🔄 Processing batch {batch_id} - Fraud Alerts")

        if not self.table_exists("gold.mart_fraud_alerts"):
            print("⏭️ Skipping fraud alerts: fraud table not ready")
            return

        fraud_df = batch_df.filter(col("fraud_flag") == True).select(
            col("event_id").alias("transaction_id"),
            "user_id",
            "merchant_id",
            col("fraud_reason").alias("alert_type"),
            "fraud_score",
            "amount_php",
            "province",
            "event_timestamp",
            lit("NEW").alias("status"),
            current_timestamp().alias("created_at")
        )

        if not fraud_df.isEmpty():
            self.write_to_postgres(fraud_df, "gold.mart_fraud_alerts")
            print(f"🚨 Generated {fraud_df.count()} fraud alerts")

    def foreach_batch_handler(self, batch_df, batch_id):
        """Process each micro-batch"""
        print(f"📦 Processing batch {batch_id}...")

        try:
            # Check table existence once
            dim_users_ready = self.table_exists("silver.dim_users")
            dim_merchants_ready = self.table_exists("silver.dim_merchants")
            fact_table_ready = dim_users_ready and dim_merchants_ready
            fraud_table_ready = self.table_exists("gold.mart_fraud_alerts")

            # --- Dimensions ---
            if dim_users_ready or dim_merchants_ready:
                self.process_dimensions(batch_df, batch_id)
            else:
                print(
                    f"⏭️ Skipping dimensions: dim_users ready? {dim_users_ready}, dim_merchants ready? {dim_merchants_ready}")

            # --- Facts ---
            if fact_table_ready:
                self.process_facts(batch_df, batch_id)
            else:
                print(
                    "⏭️ Skipping fact_transactions: Required dimension tables not ready")

            # --- Fraud alerts ---
            if fraud_table_ready:
                self.process_fraud_alerts(batch_df, batch_id)
            else:
                print("⏭️ Skipping fraud alerts: fraud table not ready")

            print(
                f"✅ Batch {batch_id} processed (with table existence checks)")

        except Exception as e:
            print(f"❌ Error processing batch {batch_id}: {e}")
            raise

    def run(self):
        """Run the streaming job"""
        print("🚀 Starting Silver Streaming Job...")

        # Read Bronze stream
        bronze_df = self.read_bronze_stream()

        # Apply transformations
        cleansed_df = self.cleanse_and_validate(bronze_df)
        silver_df = self.calculate_fraud_score(cleansed_df)

        # Write to Silver layer
        query = silver_df.writeStream \
            .foreachBatch(self.foreach_batch_handler) \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/checkpoint/silver") \
            .trigger(processingTime='10 seconds') \
            .start()

        print("✅ Streaming query started")
        query.awaitTermination()


if __name__ == "__main__":
    job = SilverStreamingJob()
    job.run()
