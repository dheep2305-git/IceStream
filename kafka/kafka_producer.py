import json
import time
import random
from datetime import datetime, timezone

from kafka import KafkaProducer


# ==========================================
# KAFKA CONFIGURATION
# ==========================================

KAFKA_SERVER = "localhost:9092"
TRANSACTIONS_TOPIC = "transactions"


# ==========================================
# SAMPLE DATA
# ==========================================

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


# ==========================================
# KAFKA PRODUCER
# ==========================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


# ==========================================
# GENERATE TRANSACTION
# ==========================================

def generate_transaction(transaction_id):

    transaction = {
        "transaction_id": transaction_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "customer_id": f"C{random.randint(1000, 9999)}",
        "product": random.choice(PRODUCTS),
        "amount": round(random.uniform(100, 50000), 2),
        "payment_method": random.choice(PAYMENT_METHODS)
    }

    # --------------------------------------
    # Generate an invalid transaction
    # Every 5th transaction = invalid amount
    # --------------------------------------

    if transaction_id % 5 == 0:
        transaction["amount"] = -500

    return transaction


# ==========================================
# MAIN
# ==========================================

def main():

    transaction_id = 1

    print()
    print("=" * 60)
    print("🚀 IceStream Kafka Producer Started")
    print("=" * 60)
    print()
    print(f"📡 Kafka Server : {KAFKA_SERVER}")
    print(f"📨 Topic        : {TRANSACTIONS_TOPIC}")
    print()
    print("Generating transactions...")
    print("Every 5th transaction will contain an invalid amount.")
    print()

    while True:

        transaction = generate_transaction(transaction_id)

        # Send transaction to Kafka
        producer.send(
            TRANSACTIONS_TOPIC,
            value=transaction
        )

        # Ensure message is delivered
        producer.flush()

        # Display transaction
        if transaction["amount"] <= 0:

            print("❌ INVALID TRANSACTION")
            print(json.dumps(transaction, indent=2))

        else:

            print("✅ VALID TRANSACTION")
            print(json.dumps(transaction, indent=2))

        print("-" * 60)

        transaction_id += 1

        # Wait 1 second before next transaction
        time.sleep(1)


# ==========================================
# START PRODUCER
# ==========================================

if __name__ == "__main__":
    main()