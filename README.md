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
During Day 4, Apache Flink was successfully set up with JobManager and TaskManager using Docker. The Kafka connector was also installed, preparing Flink for real-time transaction processing

# 📅 Day 5 — Data Quality Validation & Dead Letter Queue

## Objective

Implemented and tested the data-quality validation layer of the **IceStream – Real-Time Lakehouse Observability** platform.

The objective of this stage was to identify invalid streaming transaction records and isolate them into a **Dead Letter Queue (DLQ)** instead of allowing invalid data to continue through the normal processing pipeline.

---

## Work Completed

- Restarted and verified the Kafka and Flink Docker services.
- Verified the Kafka `transactions` topic.
- Tested the transaction producer with streaming data.

- Added intentional invalid transaction generation.
- Implemented transaction validation rules.
- Classified incoming records as GOOD DATA or BAD DATA.
- Created the `transactions_dlq` Kafka topic.
- Sent invalid transactions to the DLQ.
- Verified invalid records using the Kafka console consumer.
- Tested the complete bad-data detection workflow.

---

## Docker Service Verification

The required IceStream infrastructure was restarted and verified.

The following services were running:

```text
Kafka
Flink JobManager
Flink TaskManager

# 📅 Day 6 — Data Reliability Dashboard Development
## Objective

Enhanced the IceStream – Real-Time Lakehouse Observability platform by developing a professional Streamlit-based data reliability dashboard.

The objective was to provide a centralized interface for monitoring streaming transaction data, data-quality metrics, pipeline health, and system status.

## Work Completed
Connected the Streamlit dashboard to the existing Kafka transaction stream.
Implemented real-time transaction monitoring.
Added total record monitoring.
Added valid and invalid record counts.
Added error-rate calculation.
Added data reliability score.
Added transaction value monitoring.
Created a live pipeline visualization.
Added infrastructure status monitoring.
Added data-quality monitoring.
Added incident status monitoring.
Added circuit-breaker status visualization.
Improved the overall dashboard layout.

# 📅 Day 7 — Data Quality Simulation & Observability Intelligence
Objective

Extended IceStream's monitoring capabilities by introducing realistic data-quality scenarios and strengthening the platform's reliability and incident-monitoring features.

## Objective
The objective was to demonstrate how IceStream behaves when the incoming streaming data contains invalid or unreliable records.

## Work Completed
Enhanced the Kafka producer to generate invalid transaction records for testing.
Added intentional negative transaction amounts.
Tested the system with invalid streaming records.
Verified invalid records using the validation layer.
Verified that invalid records are sent to the DLQ Kafka topic.
Added realistic data-quality scenarios.
Added circuit-breaker monitoring.
Added incident detection states.
Added transaction intelligence metrics.
Added validation-success monitoring.

To simulate real-world data-quality problems, the Kafka producer was configured to intentionally generate invalid transaction records.

An invalid transaction can contain:

amount = -500

The validation layer identifies this as an invalid amount.

Incoming Transaction
        ↓
Data Validation
        ↓
   ┌────┴────┐
   ↓         ↓
 VALID     INVALID
   ↓         ↓
 Good       DLQ

This allows IceStream to demonstrate how bad data can be detected and isolated.

🚨 Dead Letter Queue Testing

Invalid transactions were successfully sent to the:

transactions_dlq

Kafka topic.

The DLQ records contain information such as:

Transaction ID
Timestamp
Customer ID
Product
Amount
Payment Method
Error reason

Example:

amount = -500
error = "Invalid amount"

## Day 8 — Live Kafka Data Integration

* Connected the IceStream dashboard directly to the Kafka `transactions` topic.
* Updated Kafka offset handling to retrieve the latest stored transaction records.
* Added controlled sampling of recent Kafka records for dashboard analysis.
* Verified that transaction data is successfully received from Kafka.

### Outcome

The IceStream dashboard can now monitor and analyze recent transaction events from the Kafka streaming layer.

## Day 9 — Data Quality Simulation

* Implemented controlled invalid-record generation in the Kafka producer.
* Every fifth transaction is intentionally generated with an invalid negative amount.
* Created a predictable **80% valid / 20% invalid** data-quality scenario.
* Used the controlled stream to test IceStream's validation and monitoring capabilities.

### Outcome

The system now has a repeatable test scenario for demonstrating data-quality monitoring and error detection.

## Day 10 — Reliability Score Implementation

* Added automatic calculation of total transaction records.
* Calculated valid and invalid record counts.
* Calculated the overall error rate.
* Added a reliability score based on the observed error rate.
* Improved empty-data handling to prevent incorrect reliability results.

### Outcome

The dashboard now provides a reliability score based on the actual quality of the monitored transaction stream.

## Day 11 — 80/20 Data Quality Validation

* Tested the Kafka-to-dashboard data flow using the controlled 80/20 data-quality scenario.
* Verified detection of transactions containing invalid negative amounts.
* Confirmed the dashboard metrics:

**Total Records:** 20
**Valid Records:** 16
**Invalid Records:** 4
**Error Rate:** 20%
**Reliability Score:** 80%

* Added visualization of valid and invalid transaction counts.

### Outcome

IceStream successfully demonstrates measurable data reliability using streaming transaction data.

## Day 12 — Reliability Intelligence Dashboard

* Developed a dedicated Reliability Intelligence section.
* Added a visual reliability score.
* Added data-quality indicators for important validation conditions.
* Added monitoring indicators for the validation engine and pipeline health.
* Improved the dashboard's presentation of data-quality information.

### Outcome

IceStream provides a centralized view of the current health and reliability of the transaction data pipeline.

## Day 13 — Automated Incident Detection

* Introduced threshold-based incident detection.
* Configured a **2% error-rate protection threshold**.
* Added visual status changes based on the observed error rate.
* Added incident information when data-quality errors exceed the configured threshold.
* Displayed the current error rate as part of the incident status.

### Outcome

IceStream can automatically identify when transaction data quality requires attention.

## Day 14 — Pipeline Protection Concept

* Designed the **Autonomous Pipeline Protection** layer.
* Introduced the circuit-breaker concept for protecting the streaming pipeline.
* Defined pipeline behavior when the error rate exceeds the configured threshold.
* Added the following protection states:

**🟢 ARMED** — Pipeline is operating normally.
**🔴 TRIGGERED** — Error rate has exceeded the protection threshold.

### Outcome

IceStream moves from passive monitoring toward automated protection of the streaming pipeline.

## Day 15 — Observability Intelligence Foundation

* Consolidated transaction monitoring, validation, reliability analysis, and incident detection into the IceStream dashboard.
* Added transaction intelligence metrics such as:

  * Total valid transaction value
  * Average transaction value
  * Validation success percentage
* Added a recent invalid-transactions view.
* Displayed transaction details and validation errors for problematic records.
* Added infrastructure health indicators for the major IceStream components.
* Established the foundation for advanced observability features.

### Next Phase

The next development phase will focus on:

* Autonomous Circuit Breaker
* Schema Drift Detection
* AI-Assisted Root-Cause Analysis
* Data Quality Analytics
* Incident Timeline
* Pipeline Lineage

### Outcome

IceStream has evolved into a **Real-Time Data Reliability and Observability Platform** for monitoring streaming transaction quality and pipeline health.

## Day 16 – Project Setup & Architecture

- Set up the IceStream project structure.
- Created separate folders for Kafka, Flink/processing, Iceberg, generator, and dashboard.
- Defined the overall real-time data pipeline architecture.
- Configured Kafka to run using Docker.
- Verified Kafka broker connectivity on `localhost:9092`.
- Created the `transactions` Kafka topic.
- Planned the `transactions_dlq` topic for invalid records.

---

## Day 17 – Kafka Producer & Real-Time Transactions

- Developed the Python Kafka producer using `kafka-python`.
- Created a transaction data structure containing:
  - Transaction ID
  - Timestamp
  - Customer ID
  - Product
  - Amount
  - Payment Method
- Implemented continuous transaction generation.
- Configured the producer to send transactions to the `transactions` topic.
- Added controlled invalid data generation.
- Generated an invalid transaction every fifth record by setting the amount to `-500`.
- Verified that transactions were successfully published to Kafka.

---

## Day 18 – Data Validation & DLQ Processing

- Developed the transaction processing component.
- Implemented validation rules for incoming transactions.
- Added checks for:
  - Missing transaction ID
  - Missing customer ID
  - Missing product
  - Missing amount
  - Invalid amount
  - Missing payment method
- Separated transactions into good and bad data.
- Implemented Kafka Dead Letter Queue (DLQ) processing.
- Created the `transactions_dlq` topic.
- Sent invalid transactions to the DLQ.
- Added error information and processing details to DLQ records.
- Verified invalid transactions were successfully routed to the DLQ.

---

## Day 19 – Streamlit Dashboard & Reliability Monitoring

- Developed the Streamlit observability dashboard.
- Added real-time transaction monitoring.
- Added Reliability Overview.
- Added SLO Health monitoring.
- Added Reliability Intelligence.
- Added Live Pipeline monitoring.
- Added Observability Analytics.
- Added Transaction Intelligence.
- Added Recent Invalid Transactions section.
- Added DLQ Monitoring.
- Added infrastructure status monitoring.
- Tested the dashboard with live Kafka data.
- Verified DLQ monitoring with test data.
- Observed:
  - DLQ Records: 3
  - DLQ Rate: 15%
  - DLQ SLO Target: ≤ 2%
  - SLO status: Breached

---

## Day 20 – Apache Iceberg Integration

- Created the Apache Iceberg storage structure.
- Developed the Iceberg table creation script.
- Created the `good_transactions` Iceberg table.
- Created the `dlq_transactions` Iceberg table.
- Defined schemas for good and rejected transactions.
- Configured the Iceberg warehouse:
  `iceberg/iceberg_warehouse`
- Installed PyIceberg and PyArrow.
- Verified that PyIceberg was successfully installed.
- Integrated Iceberg-related processing into the transaction processor.
- Tested the Iceberg catalog connection.
- Identified a catalog configuration issue with PyIceberg 0.12.0.
- Continued troubleshooting the Spark–Iceberg connection and write process.

### Current Status

- Kafka pipeline: **Working**
- Transaction generation: **Working**
- Data validation: **Working**
- DLQ processing: **Working**
- Streamlit dashboard: **Working**
- PyIceberg installation: **Completed**
- Iceberg connection/write verification: **In Progress**

## Three Key Innovations

### 1. Real-Time Data Quality & Validation
IceStream continuously validates streaming transaction data as it enters the pipeline. It detects issues such as missing fields and invalid transaction amounts before the data reaches downstream storage.

### 2. Autonomous Bad-Data Isolation using DLQ
Instead of allowing invalid records to interrupt the main pipeline, IceStream automatically identifies failed records and routes them to a dedicated Kafka Dead Letter Queue (`transactions_dlq`) along with error and processing information.

### 3. Real-Time Reliability & Observability
IceStream provides a Streamlit observability layer that monitors transaction flow, invalid records, DLQ activity, reliability metrics, and SLO status in real time, allowing data-quality problems to be identified from the dashboard.

## 📅 Day 21 – Iceberg Storage Integration & Troubleshooting

### 🎯 Today's Goal
Integrate Apache Iceberg with the IceStream pipeline to store valid transactions and rejected DLQ transactions as structured tables.

### ✅ Work Completed

- Configured **PySpark 4.0.1** for the IceStream project.
- Added the Apache Iceberg Spark runtime dependency:
  - `iceberg-spark-runtime-4.0_2.13:1.12.0`
- Updated `iceberg/create_tables.py` to configure an Iceberg Hadoop catalog.
- Defined the `good_transactions` Iceberg table for valid transaction data.
- Defined the `dlq_transactions` Iceberg table for rejected transaction data.
- Verified that the Iceberg dependency was successfully resolved and loaded from Maven Central.
- Tested Spark and Iceberg table creation from the IceStream project root.

### 🗂️ Iceberg Tables

#### Good Transactions
Stores successfully validated transactions.

```text
local.good_transactions

## Day 22 – IceStream Dashboard Enhancements

### Work Completed
- Enhanced the IceStream Streamlit dashboard with interactive navigation.
- Added clickable dashboard sections for:
  - Overview
  - Live Monitoring
  - Data Quality
  - DLQ Monitoring
  - SLO & Reliability
  - Pipeline Health
  - Transaction Intelligence
  - Anomaly Detection
  - Incidents
  - Analytics
  - Infrastructure
  - Search Transactions
  - Settings
- Added real-time SLO and reliability monitoring.
- Added incident detection based on error-rate and DLQ SLO breaches.
- Added severity classification for reliability incidents.
- Added anomaly detection for unusual transaction amounts and elevated error rates.
- Added pipeline health visibility from transaction generation through Kafka validation and DLQ.
- Preserved the original dashboard functionality while adding the enhanced monitoring features.
- Tested the dashboard with intentionally invalid transactions to verify SLO breach and incident detection.

### Current Demo Behavior
The current producer intentionally generates invalid transactions to test the reliability features. This can produce a 20% error rate, causing the dashboard to correctly identify an SLO breach and raise a Critical incident.

### Next Step
- Adjust the transaction generator so the normal operating state represents realistic data quality of approximately 98–99%.
- Keep the higher error-rate scenario available for demonstrating anomaly detection, SLO breach, incident creation, and DLQ isolation.

<!-- ========================================================= -->
<!-- DAY 23 - REALISTIC DATA QUALITY & FAILURE MONITORING      -->
<!-- ========================================================= -->

## Day 23 – Realistic Data Quality & Failure Monitoring

<!--
Today the IceStream data generator was improved so that the
normal operating environment behaves more realistically.
Instead of intentionally generating bad data every 5th
transaction, invalid transactions are now generated using
a configurable probability.
-->

### Work Completed

<!--
Changed the Kafka producer from a fixed invalid-data pattern
to a configurable invalid-data rate.
-->

- Updated `kafka/kafka_producer.py` to support configurable invalid-data generation.

<!--
Normal mode uses approximately 2% invalid transactions,
which represents approximately 98% valid data over a
sufficiently large number of transactions.
-->

- Configured the normal operating mode with an invalid-data rate of approximately **2%**.
- Normal mode therefore targets approximately **98% valid transaction data**.

<!--
A separate failure demonstration mode was added so that
the project can still demonstrate how IceStream behaves
when a large amount of bad data enters the pipeline.
-->

- Added a separate **Failure Demo Mode** with an approximately **20% invalid-data rate**.
- Preserved the ability to intentionally create a high-error scenario for demonstrating SLO breaches and incident detection.

<!--
The producer now clearly displays which operating mode is
currently active when it starts.
-->

- Added producer status messages showing:
  - Normal Mode
  - Failure Demo Mode
  - Configured invalid-data rate
  - Expected valid-data percentage

<!--
The existing transaction structure was preserved.
Kafka continues to receive the same transaction fields:
transaction_id, timestamp, customer_id, product, amount,
and payment_method.
-->

- Preserved the existing transaction structure and Kafka topic configuration.
- Continued sending transactions to the `transactions` Kafka topic.

### Data Quality Monitoring

<!--
The dashboard was tested with the new normal operating mode.
The initial test used only 20 records, so one invalid record
represented 5% of the sample.
-->

- Tested the dashboard using the new normal operating mode.
- Verified that invalid transactions are detected by the validation layer.
- Verified that invalid transactions are isolated through the DLQ process.

<!--
With 20 records and 1 invalid record:
Error Rate = (1 / 20) × 100 = 5%
-->

- During the initial 20-record test:
  - Total Records: **20**
  - Valid Records: **19**
  - Invalid Records: **1**
  - Error Rate: **5%**

<!--
The 5% value does not mean that the producer is configured
for a 5% invalid rate. Because invalid data is generated
randomly at approximately 2%, a small sample can temporarily
show a higher or lower percentage.
-->

- The initial 5% error rate was treated as a small-sample result because only 20 transactions were available.

### SLO Monitoring

<!--
IceStream currently uses an Error Rate SLO of <= 2%.
Therefore, the initial 5% error rate exceeded the configured
SLO.
-->

- Verified the configured Error Rate SLO:
  - Target: **≤ 2%**
  - Initial Actual: **5%**
  - Status: **BREACHED**

<!--
The Valid Records SLO requires at least 98% valid records.
The initial test had 19 valid records out of 20:
19 / 20 × 100 = 95%
-->

- Verified the Valid Records SLO:
  - Target: **≥ 98%**
  - Initial Actual: **95%**
  - Status: **BREACHED**

<!--
Required-field validation remained healthy during the test.
-->

- Required Fields:
  - Target: **≥ 99%**
  - Actual: **100%**
  - Status: **HEALTHY**

### Incident Monitoring

<!--
Because the Error Rate and DLQ Rate exceeded their configured
SLO thresholds, IceStream generated an active incident.
-->

- Verified automatic incident detection from SLO and validation signals.
- The initial test generated a **HIGH severity** incident.
- The incident identified:
  - Error Rate: **5%**
  - DLQ Rate: **5%**
  - Affected Records: **1**
  - Primary Signal: **Invalid transaction amount**

<!--
The incident recommendation directs the operator to inspect
the DLQ records and validate the producer data.
-->

- Verified the recommended incident action:
  - Inspect DLQ records.
  - Validate producer data.

### Reliability Flow Tested

<!--
The complete reliability flow tested today is:
-->

```text
Transaction Generator
        ↓
Kafka Producer
        ↓
Kafka - transactions
        ↓
Data Validation
        ↓
   ┌───────────────┐
   │               │
Valid Data      Invalid Data
   │               │
   ↓               ↓
Continue         Kafka DLQ
                    │
                    ↓
              SLO Monitoring
                    │
                    ↓
              Incident Detection
                    │
                    ↓
               Dashboard

<!-- ========================================================= -->
<!-- DAY 24 - RELIABILITY VALIDATION & INCIDENT MONITORING     -->
<!-- ========================================================= -->

## Day 24 – Reliability Validation & Incident Monitoring

<!--
Today the IceStream pipeline was tested over a larger number
of transactions to evaluate whether the normal operating mode
behaves consistently with the configured data-quality targets.
-->

### Work Completed

<!--
The Kafka producer was continued in Normal Mode with an
approximately 2% invalid-data probability.
-->

- Continued testing the Kafka producer in **Normal Mode**.
- Normal Mode uses an approximately **2% invalid-data rate**.
- The target is approximately **98% valid transaction data** over a sufficiently large sample.

<!--
The producer, validation processor, Kafka DLQ and Streamlit
dashboard were run together as an end-to-end reliability test.
-->

- Tested the complete IceStream pipeline:
  - Kafka Producer
  - Kafka `transactions` topic
  - Data Validation
  - Kafka DLQ
  - SLO Monitoring
  - Incident Detection
  - Streamlit Dashboard

### Reliability Validation

<!--
A larger transaction sample is required because a very small
sample can make the observed error percentage fluctuate
significantly.
-->

- Observed the reliability metrics over a larger transaction sample.
- Compared the observed error rate against the configured **2% Error Rate SLO**.
- Compared valid-record percentage against the configured **98% Valid Records SLO**.
- Verified that required-field validation continues to operate.

### SLO Monitoring

<!--
The SLO system continues to classify each metric as Healthy
or Breached based on its configured target.
-->

- Verified the following SLO objectives:

| SLO Objective | Target |
|---|---:|
| Valid Records | ≥ 98% |
| Error Rate | ≤ 2% |
| Required Fields | ≥ 99% |
| DLQ Rate | ≤ 2% |

<!--
SLO status is based on measured pipeline data rather than
manually changing the dashboard status.
-->

- Verified that the dashboard automatically identifies SLO breaches.
- Verified that healthy metrics are displayed separately from breached metrics.

### Incident Monitoring

<!--
IceStream generates an incident when configured reliability
signals exceed their SLO thresholds.
-->

- Verified automatic incident detection from:
  - Error Rate
  - DLQ Rate
  - Validation failures

<!--
Incident information provides operational context instead of
only displaying a numerical error percentage.
-->

- Verified incident information including:
  - Severity
  - Trigger
  - Error Rate
  - DLQ Rate
  - Affected Records
  - Primary Signal
  - Recommended Action

### Data Quality Flow

```text
Transaction
     ↓
Kafka
     ↓
Validation
     ↓
 ┌───────────────┐
 │               │
Valid           Invalid
 │               │
 ↓               ↓
Continue         DLQ
 │               │
 └───────┬───────┘
         ↓
   SLO Monitoring
         ↓
 Incident Detection
         ↓
    Dashboard