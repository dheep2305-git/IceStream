GOOD_DATA_SCHEMA = {
    "transaction_id": "integer",
    "timestamp": "string",
    "customer_id": "string",
    "product": "string",
    "amount": "double",
    "payment_method": "string"
}


DLQ_SCHEMA = {
    "transaction_id": "integer",
    "timestamp": "string",
    "customer_id": "string",
    "product": "string",
    "amount": "double",
    "payment_method": "string",
    "error": "string"
}


def print_schemas():

    print("========== GOOD DATA TABLE ==========")

    for column, data_type in GOOD_DATA_SCHEMA.items():
        print(f"{column}: {data_type}")

    print("\n========== DLQ TABLE ==========")

    for column, data_type in DLQ_SCHEMA.items():
        print(f"{column}: {data_type}")


if __name__ == "__main__":
    print_schemas()