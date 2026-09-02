import json
import random
import time
from datetime import datetime, timezone


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

    print("🚀 IceStream Transaction Generator Started")
    print("Generating transactions...\n")

    while True:
        transaction = generate_transaction(transaction_id)

        print(json.dumps(transaction))

        transaction_id += 1

        time.sleep(1)


if __name__ == "__main__":
    main()