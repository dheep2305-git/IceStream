import json
import time
import random
from datetime import datetime, timezone

from kafka import KafkaProducer


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


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


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


def main():

    transaction_id = 1

    print("🚀 IceStream Kafka Producer Started")
    print("Sending transactions to Kafka...\n")

    while True:

        transaction = generate_transaction(transaction_id)

        producer.send(
            "transactions",
            value=transaction
        )

        producer.flush()

        print("Sent:", transaction)

        transaction_id += 1

        time.sleep(1)


if __name__ == "__main__":
    main()