import json
import os

from kafka import KafkaConsumer, KafkaProducer


# ============================================================
# KAFKA CONFIGURATION
# ============================================================

KAFKA_SERVER = "localhost:9092"

INPUT_TOPIC = "transactions"

DLQ_TOPIC = "transactions_dlq"

CONSUMER_GROUP = "icestream-flink-processor"


# ============================================================
# ICEBERG CONFIGURATION
# ============================================================

BASE_DIR = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)
    )
)

ICEBERG_WAREHOUSE = os.path.join(
    BASE_DIR,
    "iceberg",
    "iceberg_warehouse"
)

print()
print("🧊 Iceberg Warehouse:")
print(ICEBERG_WAREHOUSE)
print()


# ============================================================
# SPARK + ICEBERG
# ============================================================

spark = None

try:

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("IceStream-Flink-Processor")
        .config(
            "spark.sql.catalog.local",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(
            "spark.sql.catalog.local.type",
            "hadoop"
        )
        .config(
            "spark.sql.catalog.local.warehouse",
            ICEBERG_WAREHOUSE
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    # Test that the Iceberg catalog is accessible
    spark.sql("SHOW TABLES IN local").show(truncate=False)

    print("🟢 Iceberg Storage  : CONNECTED")
    print()

except Exception as e:

    print("🔴 Iceberg Storage  : NOT CONNECTED")
    print()
    print("Iceberg Error:")
    print(str(e))
    print()

    spark = None


# ============================================================
# KAFKA PRODUCER
# ============================================================

producer = KafkaProducer(

    bootstrap_servers=KAFKA_SERVER,

    value_serializer=lambda value:
        json.dumps(value).encode("utf-8")
)


# ============================================================
# TRANSACTION VALIDATION
# ============================================================

def validate_transaction(transaction):

    errors = []

    # --------------------------------------------------------
    # Transaction must be a dictionary
    # --------------------------------------------------------

    if not isinstance(transaction, dict):

        errors.append(
            "Invalid transaction format"
        )

        return errors


    # --------------------------------------------------------
    # Transaction ID
    # --------------------------------------------------------

    if not transaction.get("transaction_id"):

        errors.append(
            "Missing transaction_id"
        )


    # --------------------------------------------------------
    # Customer ID
    # --------------------------------------------------------

    if not transaction.get("customer_id"):

        errors.append(
            "Missing customer_id"
        )


    # --------------------------------------------------------
    # Product
    # --------------------------------------------------------

    if not transaction.get("product"):

        errors.append(
            "Missing product"
        )


    # --------------------------------------------------------
    # Amount
    # --------------------------------------------------------

    amount = transaction.get("amount")

    if amount is None:

        errors.append(
            "Missing amount"
        )

    else:

        try:

            amount = float(amount)

            if amount <= 0:

                errors.append(
                    "Invalid amount"
                )

        except (ValueError, TypeError):

            errors.append(
                "Invalid amount"
            )


    # --------------------------------------------------------
    # Payment Method
    # --------------------------------------------------------

    if not transaction.get("payment_method"):

        errors.append(
            "Missing payment_method"
        )


    return errors


# ============================================================
# WRITE GOOD TRANSACTION TO ICEBERG
# ============================================================

def write_good_transaction(transaction):

    if spark is None:

        return False

    try:

        record = {

            "transaction_id":
                int(
                    transaction.get(
                        "transaction_id"
                    )
                ),

            "timestamp":
                str(
                    transaction.get(
                        "timestamp",
                        ""
                    )
                ),

            "customer_id":
                str(
                    transaction.get(
                        "customer_id",
                        ""
                    )
                ),

            "product":
                str(
                    transaction.get(
                        "product",
                        ""
                    )
                ),

            "amount":
                float(
                    transaction.get(
                        "amount"
                    )
                ),

            "payment_method":
                str(
                    transaction.get(
                        "payment_method",
                        ""
                    )
                )
        }


        # Create Spark DataFrame
        df = spark.createDataFrame(
            [record]
        )


        # Append to existing Iceberg table
        (
            df.writeTo(
                "local.good_transactions"
            )
            .append()
        )


        return True


    except Exception as e:

        print()
        print("⚠️ ICEBERG GOOD DATA WRITE FAILED")
        print(
            f"Error: {str(e)}"
        )
        print()

        return False


# ============================================================
# WRITE DLQ TRANSACTION TO ICEBERG
# ============================================================

def write_dlq_transaction(dlq_record):

    if spark is None:

        return False

    try:

        # ----------------------------------------------------
        # IMPORTANT:
        # This matches the schema created in create_table.py
        #
        # transaction_id
        # timestamp
        # customer_id
        # product
        # amount
        # payment_method
        # error
        # ----------------------------------------------------

        record = {

            "transaction_id":
                int(
                    dlq_record.get(
                        "transaction_id"
                    )
                ),

            "timestamp":
                str(
                    dlq_record.get(
                        "timestamp",
                        ""
                    )
                ),

            "customer_id":
                str(
                    dlq_record.get(
                        "customer_id",
                        ""
                    )
                ),

            "product":
                str(
                    dlq_record.get(
                        "product",
                        ""
                    )
                ),

            "amount":
                float(
                    dlq_record.get(
                        "amount"
                    )
                ),

            "payment_method":
                str(
                    dlq_record.get(
                        "payment_method",
                        ""
                    )
                ),

            "error":
                str(
                    dlq_record.get(
                        "error",
                        ""
                    )
                )
        }


        # Create Spark DataFrame
        df = spark.createDataFrame(
            [record]
        )


        # Append to Iceberg DLQ table
        (
            df.writeTo(
                "local.dlq_transactions"
            )
            .append()
        )


        return True


    except Exception as e:

        print()
        print("⚠️ ICEBERG DLQ WRITE FAILED")
        print(
            f"Error: {str(e)}"
        )
        print()

        return False


# ============================================================
# KAFKA CONSUMER
# ============================================================

consumer = KafkaConsumer(

    INPUT_TOPIC,

    bootstrap_servers=KAFKA_SERVER,

    value_deserializer=lambda value:
        json.loads(
            value.decode("utf-8")
        ),

    auto_offset_reset="latest",

    enable_auto_commit=True,

    group_id=CONSUMER_GROUP
)


# ============================================================
# PROCESSOR START
# ============================================================

print()
print("=" * 65)
print("🚀 IceStream Flink Processor Started")
print("=" * 65)
print()

print(
    f"📨 Input Topic      : {INPUT_TOPIC}"
)

print(
    f"🚨 DLQ Topic        : {DLQ_TOPIC}"
)

print(
    f"📡 Kafka            : {KAFKA_SERVER}"
)

print(
    f"👥 Consumer Group   : {CONSUMER_GROUP}"
)

print(
    f"🧊 Iceberg Warehouse: {ICEBERG_WAREHOUSE}"
)

print()

if spark is not None:

    print(
        "🟢 Iceberg Storage  : CONNECTED"
    )

else:

    print(
        "🔴 Iceberg Storage  : NOT CONNECTED"
    )

print()

print("Waiting for transactions...")
print()


# ============================================================
# TRANSACTION PROCESSING LOOP
# ============================================================

for message in consumer:

    transaction = {}

    try:

        # ----------------------------------------------------
        # RECEIVE TRANSACTION
        # ----------------------------------------------------

        transaction = message.value

        print("-" * 65)

        print("📥 TRANSACTION RECEIVED")

        print(
            json.dumps(
                transaction,
                indent=2
            )
        )


        # ----------------------------------------------------
        # VALIDATE
        # ----------------------------------------------------

        errors = validate_transaction(
            transaction
        )


        # ====================================================
        # GOOD TRANSACTION
        # ====================================================

        if not errors:

            print()
            print("✅ GOOD DATA")

            print(
                "Validation Status: PASSED"
            )

            print(
                f"Transaction ID: "
                f"{transaction.get('transaction_id')}"
            )


            # ------------------------------------------------
            # WRITE TO ICEBERG
            # ------------------------------------------------

            iceberg_success = (
                write_good_transaction(
                    transaction
                )
            )


            if iceberg_success:

                print(
                    "🧊 STORED IN ICEBERG"
                )

                print(
                    "Table: "
                    "local.good_transactions"
                )

            else:

                print(
                    "⚠️ GOOD DATA NOT STORED "
                    "IN ICEBERG"
                )


            print()


        # ====================================================
        # BAD TRANSACTION
        # ====================================================

        else:

            print()
            print("❌ BAD DATA")

            print(
                "Validation Status: FAILED"
            )

            print(
                "Errors:"
            )


            for error in errors:

                print(
                    f"   • {error}"
                )


            # ------------------------------------------------
            # CREATE DLQ RECORD
            # ------------------------------------------------

            dlq_record = {

                "transaction_id":
                    transaction.get(
                        "transaction_id"
                    ),

                "timestamp":
                    transaction.get(
                        "timestamp"
                    ),

                "customer_id":
                    transaction.get(
                        "customer_id"
                    ),

                "product":
                    transaction.get(
                        "product"
                    ),

                "amount":
                    transaction.get(
                        "amount"
                    ),

                "payment_method":
                    transaction.get(
                        "payment_method"
                    ),

                "error":
                    ", ".join(errors),

                "error_count":
                    len(errors),

                "source_topic":
                    INPUT_TOPIC,

                "processing_status":
                    "FAILED",

                "dlq_topic":
                    DLQ_TOPIC
            }


            # ------------------------------------------------
            # SEND TO KAFKA DLQ
            # ------------------------------------------------

            producer.send(
                DLQ_TOPIC,
                value=dlq_record
            )

            producer.flush()


            print()
            print("🚨 SENT TO KAFKA DLQ")

            print(
                json.dumps(
                    dlq_record,
                    indent=2
                )
            )


            # ------------------------------------------------
            # WRITE TO ICEBERG DLQ
            # ------------------------------------------------

            iceberg_dlq_success = (
                write_dlq_transaction(
                    dlq_record
                )
            )


            if iceberg_dlq_success:

                print(
                    "🧊 STORED IN ICEBERG DLQ"
                )

                print(
                    "Table: "
                    "local.dlq_transactions"
                )

            else:

                print(
                    "⚠️ DLQ RECORD NOT STORED "
                    "IN ICEBERG"
                )


            print()


    # ========================================================
    # PROCESSING ERROR
    # ========================================================

    except Exception as e:

        print()
        print("🔥 PROCESSING ERROR")

        print(
            f"Error: {str(e)}"
        )

        print()


        # ----------------------------------------------------
        # SEND PROCESSING ERROR TO DLQ
        # ----------------------------------------------------

        try:

            error_record = {

                "transaction_id":
                    transaction.get(
                        "transaction_id",
                        0
                    ),

                "timestamp":
                    transaction.get(
                        "timestamp",
                        ""
                    ),

                "customer_id":
                    transaction.get(
                        "customer_id",
                        ""
                    ),

                "product":
                    transaction.get(
                        "product",
                        ""
                    ),

                "amount":
                    transaction.get(
                        "amount",
                        0
                    ),

                "payment_method":
                    transaction.get(
                        "payment_method",
                        ""
                    ),

                "error":
                    f"Processor Error: {str(e)}",

                "error_count":
                    1,

                "source_topic":
                    INPUT_TOPIC,

                "processing_status":
                    "PROCESSING_ERROR",

                "dlq_topic":
                    DLQ_TOPIC
            }


            producer.send(
                DLQ_TOPIC,
                value=error_record
            )

            producer.flush()


            print(
                "🚨 PROCESSING ERROR SENT TO KAFKA DLQ"
            )


            # ------------------------------------------------
            # STORE PROCESSING ERROR IN ICEBERG
            # ------------------------------------------------

            write_dlq_transaction(
                error_record
            )


        except Exception as dlq_error:

            print(
                "❌ FAILED TO SEND ERROR TO DLQ"
            )

            print(
                f"DLQ Error: {str(dlq_error)}"
            )