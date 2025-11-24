from synthetic_data_crafter import SyntheticDataCrafter
from datetime import datetime
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError

import random
import os
import argparse
import json
import time

KAFKA_CONFIG = {
    'bootstrap_servers': os.getenv(
        'KAFKA_BOOTSTRAP_SERVERS',
        'redpanda:9092'
    ).split(','),

    'topic_name': os.getenv(
        'KAFKA_TOPIC',
        'walletflow-bronze-transactions'
    ),

    'topic_config': {
        'partitions': int(os.getenv('KAFKA_PARTITIONS', '6')),
        'replication_factor': int(os.getenv('KAFKA_REPLICATION', '1')),
        # 7 days
        'retention_ms': int(os.getenv('KAFKA_RETENTION_MS', '604800000')),
        'compression_type': os.getenv('KAFKA_COMPRESSION', 'gzip'),
        # 1MB
        'max_message_bytes': int(os.getenv('KAFKA_MAX_MESSAGE_BYTES', '1048576')),
    },

    'producer_config': {
        'acks': os.getenv('KAFKA_ACKS', 'all'),
        'retries': int(os.getenv('KAFKA_RETRIES', '3')),
        'max_in_flight_requests_per_connection': 5,
        'compression_type': os.getenv('KAFKA_COMPRESSION', 'gzip'),
        'batch_size': int(os.getenv('KAFKA_BATCH_SIZE', '16384')),
        'linger_ms': int(os.getenv('KAFKA_LINGER_MS', '10')),
    },

    'consumer_config': {
        'group_id': os.getenv('KAFKA_CONSUMER_GROUP', 'walletflow-spark-streaming'),
        'auto_offset_reset': os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest'),
        'enable_auto_commit': False,
        'max_poll_records': int(os.getenv('KAFKA_MAX_POLL_RECORDS', '500')),
    }
}

try:
    KAFKA_BOOTSTRAP_SERVERS = KAFKA_CONFIG['bootstrap_servers']
    KAFKA_TOPIC = KAFKA_CONFIG['topic_name']
    PRODUCER_CONFIG = KAFKA_CONFIG['producer_config']
except ImportError:
    KAFKA_BOOTSTRAP_SERVERS = ['redpanda:9092']
    KAFKA_TOPIC = 'walletflow-bronze-transactions'
    PRODUCER_CONFIG = {
        'acks': 'all',
        'retries': 3,
        'compression_type': 'gzip'
    }


AMOUNT_RANGES = [
    (20, 200, 0.35),      # Micro: convenience stores
    (200, 1000, 0.30),    # Small: fast food, transport
    (1000, 5000, 0.20),   # Medium: groceries, bills
    (5000, 20000, 0.12),  # Large: restaurants, shopping
    (20000, 50000, 0.03)  # XLarge: appliances, remittance
]

PROVINCE_CITIES = {
    "Metro Manila": [
        "Quezon City", "Manila", "Makati", "Pasig", "Taguig",
        "Caloocan", "Parañaque", "Valenzuela", "Pasay", "Las Piñas"
    ],
    "Cebu": [
        "Cebu City", "Mandaue", "Lapu-Lapu City", "Talisay"
    ],
    "Davao del Sur": [
        "Davao City"
    ],
    "Cavite": [
        "Bacoor", "Imus", "Dasmariñas", "Trece Martires", "General Trias"
    ],
    "Laguna": [
        "Santa Rosa", "Calamba", "San Pedro", "Biñan"
    ],
    "Bulacan": [
        "Malolos", "Meycauayan", "San Jose del Monte"
    ],
    "Pampanga": [
        "San Fernando", "Angeles City", "Mabalacat"
    ],
    "Batangas": [
        "Batangas City", "Lipa"
    ],
    "Rizal": [
        "Antipolo", "Cainta", "Taytay", "Binangonan"
    ],
    "Negros Occidental": [
        "Bacolod", "Silay", "Talisay"
    ],
    "Iloilo": [
        "Iloilo City", "Passi"
    ],
    "Benguet": [
        "Baguio"
    ],
    "Pangasinan": [
        "Dagupan", "Urdaneta", "San Carlos"
    ],
    "Misamis Oriental": [
        "Cagayan de Oro"
    ],
    "Zamboanga del Sur": [
        "Zamboanga City"
    ],
    "South Cotabato": [
        "General Santos City"
    ],
    "Leyte": [
        "Tacloban", "Ormoc"
    ],
    "Palawan": [
        "Puerto Princesa"
    ],
    "Bohol": [
        "Tagbilaran"
    ]
}


MERCHANTS = {
    "Food": ["Jollibee", "McDonald's", "Chowking", "Mang Inasal", "KFC",
             "Army Navy", "BonChon", "Shakey's", "Max's Restaurant", "Yellow Cab"],
    "Groceries": ["Puregold", "SM Supermarket", "Robinsons Supermarket",
                  "Savemore", "Waltermart", "Landmark"],
    "Transport": ["Grab", "Angkas", "JoyRide", "Lalamove", "Move It"],
    "Bills": ["Electric", "Water", "Globe", "Smart", "PLDT", "Converge"],
    "Remittance": ["Palawan Pawnshop", "Cebuana Lhuillier", "MLhuillier", "Western Union"],
    "E-commerce": ["Shopee", "Lazada", "TikTok Shop", "Zalora"],
    "Entertainment": ["Netflix PH", "Spotify", "GCash GMovies", "Cignal TV"]
}

SCHEMA = [
    {
        "label": "event_id",
        "key_label": "uuid_v4",
        "group": 'it',
        "options": {"blank_percentage": 0}
    },
    {
        "label": "event_timestamp",
        "key_label": "current_timestamp",
        "group": 'basic',
        "options": {"blank_percentage": 0, 'format': "%Y-%m-%dT%H:%M:%SZ"}
    },
    {
        "label": "transaction_type",
        "key_label": "custom_list",
        "group": 'basic',
        "options": {"blank_percentage": 0, 'custom_format': 'PAYMENT,TRANSFER,CASHOUT,CASHIN,BILLPAY'}
    },
    {
        'label': "user_id",
        "key_label": "character_sequence",
        'group': "advanced",
        "options": {"blank_percentage": 0, 'format': 'USER-########'}
    },
    {
        'label': "user_name",
        "key_label": "full_name",
        'group': "personal",
        "options": {"blank_percentage": 0}
    },
    {
        'label': "merchant_id",
        "key_label": "character_sequence",
        'group': "advanced",
        "options": {"blank_percentage": 0, 'format': 'M-########'}
    },
    {
        'label': "device_id",
        "key_label": "character_sequence",
        'group': "advanced",
        "options": {"blank_percentage": 0, 'format': 'DEVICE_####_#####'}
    },
    {
        'label': "ip_address",
        "key_label": "ip_address_v4",
        'group': "it",
        "options": {"blank_percentage": 0}
    }
]


class TransactionGenerator:
    def __init__(self, bootstrap_servers=None, topic=None):
        self.topic = topic or KAFKA_TOPIC

        # Use provided servers or fall back to config
        servers = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS

        self.producer = KafkaProducer(
            bootstrap_servers=servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            **PRODUCER_CONFIG  # Unpack config settings
        )
        self.tx_count = 0
        self.error_count = 0

        # User session tracking (for realistic patterns)
        self.active_users = {}

        print(f"✅ Kafka Producer connected to {bootstrap_servers}")
        print(f"📤 Publishing to topic: {topic}")

    def _select_amount(self):
        ranges = [r for r in AMOUNT_RANGES]
        weights = [r[2] for r in ranges]
        selected = random.choices(ranges, weights=weights, k=1)[0]
        return round(random.uniform(selected[0], selected[1]), 2)

    def _select_merchant(self):
        category_weights = {
            "Food": 0.30,
            "Groceries": 0.20,
            "Transport": 0.15,
            "Bills": 0.15,
            "E-commerce": 0.10,
            "Remittance": 0.07,
            "Entertainment": 0.03
        }

        category = random.choices(
            list(category_weights.keys()),
            weights=list(category_weights.values()),
            k=1
        )[0]

        merchant_name = random.choice(MERCHANTS[category])
        return category, merchant_name

    def _update_user_session(self, user_id):
        """Track user transactions for velocity detection"""
        now = time.time()
        if user_id not in self.active_users:
            self.active_users[user_id] = []

        # Add current transaction
        self.active_users[user_id].append(now)

        # Keep only last 5 minutes
        self.active_users[user_id] = [
            t for t in self.active_users[user_id]
            if now - t < 300
        ]

    def _detect_fraud_patterns(self, user_id, amount, hour):
        """Simple fraud pattern detection"""
        fraud_reason = None

        # Rule 1: High amount at night
        if amount > 50000 and hour in range(0, 5):
            fraud_reason = "HIGH_AMOUNT_NIGHT"

        # Rule 2: Velocity spike (simplified - check active users)
        elif user_id in self.active_users:
            recent_tx = self.active_users[user_id]
            if len(recent_tx) >= 10:  # More than 10 tx in window
                fraud_reason = "VELOCITY_SPIKE"

        # Rule 3: Round amount suspicion
        elif amount > 10000 and amount % 1000 == 0:
            fraud_reason = "ROUND_AMOUNT"

        # Rule 4: Random high-risk (simulate ML model)
        elif amount > 30000 and random.random() < 0.05:
            fraud_reason = "HIGH_RISK_ML_SCORE"

        return fraud_reason

    def generate_transaction(self):
        province = random.choice(list(PROVINCE_CITIES.keys()))
        city = random.choice(PROVINCE_CITIES[province])
        is_international = random.random() < 0.02
        category, merchant_name = self._select_merchant()
        current_hour = datetime.now().strftime("%H")

        d = [
            {
                **row,
                "amount_php": self._select_amount(),
                "merchant_name": merchant_name,
                "merchant_category": category,
                "province": province,
                "city": city,
                "is_international": is_international,
            }
            for row in SyntheticDataCrafter(SCHEMA).one().data
        ]
        user_id = d[0]['user_id']
        amount = d[0]['amount_php']
        self._update_user_session(user_id)
        fraud_reason = self._detect_fraud_patterns(
            user_id, amount, int(current_hour))

        return d, fraud_reason

    def send_transaction(self, event):
        try:
            # Extract first element if it's a list
            data = event[0] if isinstance(event, list) else event
            future = self.producer.send(self.topic, value=data)
            future.get(timeout=10)
            self.tx_count += 1
            return True
        except KafkaError as e:
            self.error_count += 1
            print(f"❌ Kafka Error: {e}")
            return False

    def run(self, duration_seconds=None, target_rate=None):
        start_time = time.time()
        last_report = start_time

        try:
            while True:
                if duration_seconds and (time.time() - start_time) >= duration_seconds:
                    print(f"\n⏰ Duration limit reached ({duration_seconds}s)")
                    break

                if target_rate:
                    batch_size = target_rate // 60  # Per second
                else:
                    batch_size = random.randint(150, 300)  # Variable load

                for _ in range(batch_size):
                    event, fraud_reason = self.generate_transaction()
                    success = self.send_transaction(event)

                    if fraud_reason:
                        tx = event[0] if isinstance(event, list) else event
                        print(f"⚠️  FRAUD: {tx['user_name']} → {tx['merchant_name']} "
                              f"₱{tx['amount_php']} [{fraud_reason}]")

                # Report progress every 10 seconds
                if time.time() - last_report >= 10:
                    elapsed = time.time() - start_time
                    rate = (self.tx_count / elapsed) * 60 if elapsed > 0 else 0
                    print(
                        f"📈 Sent: {self.tx_count:,} tx | Rate: {rate:,.0f} tx/min | Errors: {self.error_count}")
                    last_report = time.time()

                # Sleep to control rate
                time.sleep(1)

        except KeyboardInterrupt:
            print("\n⛔ Generator stopped by user")

        finally:
            self.producer.flush()
            self.producer.close()

            # Final report
            elapsed = time.time() - start_time
            avg_rate = (self.tx_count / elapsed) * 60 if elapsed > 0 else 0
            print("\n" + "=" * 70)
            print(f"📊 FINAL STATISTICS")
            print(f"   Total Transactions: {self.tx_count:,}")
            print(f"   Total Errors: {self.error_count}")
            print(f"   Runtime: {elapsed:.1f}s")
            print(f"   Average Rate: {avg_rate:,.0f} tx/min")
            print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description='WalletFlow Transaction Generator')
    parser.add_argument('--duration', type=int,
                        help='Run duration in seconds', default=None)
    parser.add_argument('--rate', type=int,
                        help='Target transactions per minute', default=None)
    parser.add_argument('--kafka', type=str,
                        help='Kafka bootstrap servers', default='redpanda:9092')
    parser.add_argument('--topic', type=str, help='Kafka topic',
                        default='walletflow-bronze-transactions')

    args = parser.parse_args()

    # Parse kafka servers
    servers = args.kafka.split(',')

    # Create and run generator
    generator = TransactionGenerator(servers, args.topic)
    generator.run(duration_seconds=args.duration, target_rate=args.rate)


if __name__ == "__main__":
    main()
