# ==========================================
# GOOD TRANSACTIONS SCHEMA
# ==========================================

GOOD_DATA_SCHEMA = {
    "transaction_id": "integer",
    "timestamp": "string",
    "customer_id": "string",
    "product": "string",
    "amount": "double",
    "payment_method": "string"
}


# ==========================================
# DLQ TRANSACTIONS SCHEMA
# ==========================================

DLQ_SCHEMA = {
    "transaction_id": "integer",
    "timestamp": "string",
    "customer_id": "string",
    "product": "string",
    "amount": "double",
    "payment_method": "string",

    # Error information
    "error": "string",
    "error_count": "integer",

    # Pipeline information
    "source_topic": "string",
    "processing_status": "string",
    "dlq_topic": "string"
}


# ==========================================
# PRINT SCHEMAS
# ==========================================

def print_schemas():

    print()
    print("=" * 50)
    print("GOOD DATA TABLE SCHEMA")
    print("=" * 50)

    for column, data_type in GOOD_DATA_SCHEMA.items():
        print(f"{column}: {data_type}")

    print()
    print("=" * 50)
    print("DLQ TABLE SCHEMA")
    print("=" * 50)

    for column, data_type in DLQ_SCHEMA.items():
        print(f"{column}: {data_type}")

    print()


# ==========================================
# RUN
# ==========================================

if __name__ == "__main__":
    print_schemas()