import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


# ==============================
# KAFKA CONFIGURATION
# ==============================

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "transactions"


# ==============================
# SAMPLE DATA
# ==============================

PRODUCTS = [
    "Laptop",
    "Mobile",
    "Headphones",
    "Keyboard",
    "Mouse",
    "Smartwatch"
]

PAYMENT_METHODS = [
    "UPI",
    "Credit Card",
    "Debit Card",
    "Net Banking"
]


# ==============================
# KAFKA PRODUCER
# ==============================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


# ==============================
# TRANSACTION GENERATOR
# ==============================

def generate_transaction(transaction_id):

    transaction = {
        "transaction_id": transaction_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "customer_id": f"C{random.randint(1000, 9999)}",
        "product": random.choice(PRODUCTS),
        "amount": round(random.uniform(100, 50000), 2),
        "payment_method": random.choice(PAYMENT_METHODS)
    }

    return transaction


# ==============================
# MAIN
# ==============================

def main():

    transaction_id = 1

    print()
    print("=" * 60)
    print("🚀 IceStream Transaction Generator Started")
    print("=" * 60)
    print()
    print(f"📡 Kafka Server : {KAFKA_SERVER}")
    print(f"📨 Kafka Topic  : {KAFKA_TOPIC}")
    print()
    print("Generating transactions...")
    print()

    while True:

        transaction = generate_transaction(transaction_id)

        # Print transaction
        print("📤 TRANSACTION SENT")
        print(json.dumps(transaction, indent=2))

        # Send transaction to Kafka
        producer.send(
            KAFKA_TOPIC,
            value=transaction
        )

        # Make sure Kafka receives it immediately
        producer.flush()

        print("✅ Sent to Kafka topic:", KAFKA_TOPIC)
        print("-" * 60)

        transaction_id += 1

        # Generate one transaction every second
        time.sleep(1)


# ==============================
# START PROGRAM
# ==============================

if __name__ == "__main__":
    main()