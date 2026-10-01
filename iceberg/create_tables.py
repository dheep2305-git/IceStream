from pyspark.sql import SparkSession


# ==========================================
# CREATE SPARK SESSION
# ==========================================

spark = (
    SparkSession.builder
    .appName("IceStream-Iceberg")
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
        "./iceberg_warehouse"
    )
    .getOrCreate()
)


# ==========================================
# GOOD TRANSACTIONS TABLE
# ==========================================

spark.sql("""
CREATE TABLE IF NOT EXISTS local.good_transactions (

    transaction_id INT,
    timestamp STRING,
    customer_id STRING,
    product STRING,
    amount DOUBLE,
    payment_method STRING

)
USING iceberg
""")


# ==========================================
# DLQ TRANSACTIONS TABLE
# ==========================================

spark.sql("""
CREATE TABLE IF NOT EXISTS local.dlq_transactions (

    transaction_id INT,
    timestamp STRING,
    customer_id STRING,
    product STRING,
    amount DOUBLE,
    payment_method STRING,

    error STRING,
    error_count INT,

    source_topic STRING,
    processing_status STRING,
    dlq_topic STRING

)
USING iceberg
""")


# ==========================================
# SUCCESS MESSAGE
# ==========================================

print()
print("=" * 60)
print("✅ IceStream Iceberg Tables Created Successfully!")
print("=" * 60)
print()

print("📊 Tables:")
print("   • local.good_transactions")
print("   • local.dlq_transactions")

print()
print("🏭 Warehouse:")
print("   ./iceberg_warehouse")
print()


# ==========================================
# STOP SPARK
# ==========================================

spark.stop()