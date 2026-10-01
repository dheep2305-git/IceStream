import json
from kafka import KafkaProducer


# ==========================================
# KAFKA CONFIGURATION
# ==========================================

KAFKA_SERVER = "localhost:9092"
DLQ_TOPIC = "transactions_dlq"
SOURCE_TOPIC = "transactions"


# ==========================================
# KAFKA PRODUCER
# ==========================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


# ==========================================
# SEND INVALID TRANSACTION TO DLQ
# ==========================================

def send_to_dlq(transaction, errors):

    dlq_record = {
        "transaction_id": transaction.get("transaction_id"),
        "timestamp": transaction.get("timestamp"),
        "customer_id": transaction.get("customer_id"),
        "product": transaction.get("product"),
        "amount": transaction.get("amount"),
        "payment_method": transaction.get("payment_method"),

        # Error information
        "error": ", ".join(errors),
        "error_count": len(errors),

        # Pipeline information
        "source_topic": SOURCE_TOPIC,
        "processing_status": "FAILED",
        "dlq_topic": DLQ_TOPIC
    }

    # Send to Kafka DLQ topic
    producer.send(
        DLQ_TOPIC,
        value=dlq_record
    )

    # Make sure message is delivered
    producer.flush()

    print()
    print("🚨 SENT TO DLQ")
    print(json.dumps(dlq_record, indent=2))
    print()


# ==========================================
# CLOSE PRODUCER
# ==========================================

def close_producer():

    producer.flush()
    producer.close()

    print("🛑 DLQ Producer Closed")