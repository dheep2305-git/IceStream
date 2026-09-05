# IceStream
IceStream is a real-time data observability system that monitors streaming data, detects data-quality problems, isolates bad records, and provides visibility into the health of the data pipeline.

# 📅 Day 1 — Project Setup

## Objective

Started the development of **IceStream – Real-Time Lakehouse Observability**.

The main objective of the project is to build a real-time system that monitors streaming data, detects data-quality problems, isolates bad records, and provides visibility into the health of the data pipeline.

## Work Completed

- Created the IceStream project repository.
- Created the basic project folder structure.
- Defined the overall project architecture.
- Identified the main technologies required for the project.
- Created the initial README documentation.

## Project Structure

```text
IceStream/
├── dashboard/
├── flink/
├── generator/
├── iceberg/
├── kafka/
└── README.md
```
# 📅 Day 2 — Project Setup

## Objective

Started implementing the main components of the IceStream real-time data pipeline by creating the transaction generator, Kafka producer, Flink validation logic, Iceberg schemas, and initial Streamlit dashboard.
- Created a Python transaction generator for continuous mock e-commerce data.
- Created the Kafka producer for sending transaction data to the `transactions` Kafka topic.
- Created Flink processing logic for validating incoming transactions.
- Added data-quality checks for required transaction fields and invalid amounts.
- Created Iceberg schemas for good data and bad data/DLQ records.
- Created the initial Streamlit dashboard for displaying IceStream monitoring information.
During Day 2, the initial components of IceStream were organized to establish a foundation for the complete real-time data pipeline. The transaction generator, Kafka producer, Flink validation logic, Iceberg schemas, and Streamlit dashboard were created separately so that each component can later be integrated into a single working system

# 📅 Day 3 — Kafka Setup and Streaming Integration
Objective

Set up Apache Kafka as the streaming layer for the IceStream project.

The goal was to create a Kafka environment, create the required transaction topic, and verify that transaction data can be continuously sent to and received from Kafka.

Work Completed
Verified Docker installation.
Started Docker Desktop and verified the Docker engine.
Downloaded Apache Kafka using Docker.
Started the Kafka broker.
Created the transactions Kafka topic.
Verified that the topic was created successfully.
Installed the kafka-python library.
Connected the Python producer to Kafka.
Sent transaction records continuously to Kafka.
Created a Kafka consumer to verify the streamed records.
Successfully confirmed that transaction messages were being stored and consumed from Kafka.
Kafka Setup.
During Day 3, Kafka was successfully configured as the messaging layer of IceStream. The transaction producer continuously generated e-commerce transaction events and sent them to the transactions topic, while the Kafka consumer was used to verify that the messages were successfully received. This confirmed that the streaming communication layer was working correctly

# 📅 Day 4 — Apache Flink Setup Objective

Set up Apache Flink as the real-time stream processing layer of IceStream.

The goal was to connect Flink with the Kafka environment and prepare Flink for real-time transaction validation and data-quality processing.

Work Completed
Verified Java installation.
Verified Java 21 was available.
Downloaded Apache Flink using Docker.
Started the Flink JobManager.
Accessed the Flink Web Dashboard.
Configured a Flink TaskManager.
Created a dedicated Docker network for IceStream.
Connected Flink JobManager and Kafka to the IceStream network.
Configured the TaskManager to communicate with the JobManager.
Successfully started the Flink TaskManager.
Installed the Kafka connector required by Flink.
Verified that the Kafka connector was available inside Flink.
Java Setup
Flink requires Java to run.
Java was verified using:
java -version
During Day 4, Apache Flink was configured as the real-time processing layer of IceStream. The JobManager and TaskManager were successfully started using Docker, and the Kafka connector was installed to prepare Flink for consuming streaming transaction data. The environment is now ready for implementing the actual Kafka-to-Flink processing and data-quality validation pipeline.


