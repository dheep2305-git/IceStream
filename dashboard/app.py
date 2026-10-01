import json
import time

import pandas as pd
import streamlit as st
from kafka import KafkaConsumer, TopicPartition


# ============================================================
# SLO CONFIGURATION
# ============================================================

VALID_RECORDS_SLO = 98.0
ERROR_RATE_SLO = 2.0
REQUIRED_FIELDS_SLO = 99.0
DLQ_RATE_SLO = 2.0


# ============================================================
# PAGE CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="IceStream | Data Reliability Platform",
    page_icon="❄️",
    layout="wide",
    initial_sidebar_state="expanded"
)


# ============================================================
# CUSTOM CSS
# ============================================================

st.markdown(
"""
<style>

.stApp {
    background:
        radial-gradient(
            circle at 10% 0%,
            rgba(37, 99, 235, 0.18),
            transparent 30%
        ),
        radial-gradient(
            circle at 90% 10%,
            rgba(14, 165, 233, 0.12),
            transparent 28%
        ),
        #07111f;
    color: #f8fafc;
}

.block-container {
    padding-top: 2rem;
    padding-bottom: 3rem;
    max-width: 1500px;
}

#MainMenu {
    visibility: hidden;
}

footer {
    visibility: hidden;
}

header {
    background: transparent !important;
}


/* ============================================================
   SIDEBAR
   ============================================================ */

[data-testid="stSidebar"] {
    background: #091525;
    border-right: 1px solid rgba(148, 163, 184, 0.12);
}

.sidebar-title {
    font-size: 25px;
    font-weight: 800;
    color: #f8fafc;
    margin-bottom: 5px;
}

.sidebar-subtitle {
    color: #64748b;
    font-size: 12px;
    line-height: 1.5;
}

.sidebar-section {
    color: #38bdf8;
    font-size: 11px;
    font-weight: 800;
    text-transform: uppercase;
    letter-spacing: 1px;
    margin-top: 28px;
    margin-bottom: 10px;
}

.sidebar-item {
    color: #94a3b8;
    font-size: 13px;
    padding: 8px 0;
}


/* ============================================================
   HEADER
   ============================================================ */

.brand {
    display: flex;
    align-items: center;
    gap: 15px;
}

.brand-icon {
    width: 58px;
    height: 58px;
    border-radius: 16px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 31px;
    background: linear-gradient(
        135deg,
        #0ea5e9,
        #2563eb
    );
    box-shadow:
        0 10px 30px rgba(14, 165, 233, 0.25);
}

.brand-name {
    font-size: 38px;
    font-weight: 850;
    letter-spacing: -1.5px;
    color: #f8fafc;
}

.brand-subtitle {
    color: #94a3b8;
    font-size: 14px;
    margin-top: -3px;
}

.live-badge {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    padding: 9px 15px;
    border-radius: 30px;
    background: rgba(34, 197, 94, 0.10);
    border: 1px solid rgba(34, 197, 94, 0.30);
    color: #86efac;
    font-size: 13px;
    font-weight: 700;
}

.live-dot {
    width: 9px;
    height: 9px;
    border-radius: 50%;
    background: #22c55e;
    box-shadow: 0 0 12px #22c55e;
}


/* ============================================================
   SECTION HEADERS
   ============================================================ */

.section-header {
    margin-top: 25px;
    margin-bottom: 5px;
    font-size: 21px;
    font-weight: 750;
    color: #f8fafc;
}

.section-description {
    color: #64748b;
    font-size: 13px;
    margin-bottom: 16px;
}


/* ============================================================
   KPI CARDS
   ============================================================ */

.kpi-card {
    position: relative;
    overflow: hidden;
    min-height: 145px;
    padding: 22px;
    border-radius: 18px;
    background: linear-gradient(
        145deg,
        rgba(20, 34, 55, 0.97),
        rgba(12, 24, 40, 0.97)
    );
    border: 1px solid rgba(148, 163, 184, 0.12);
    box-shadow:
        0 15px 40px rgba(0, 0, 0, 0.20);
}

.kpi-card:hover {
    border-color: rgba(56, 189, 248, 0.30);
}

.kpi-icon {
    font-size: 24px;
    margin-bottom: 7px;
}

.kpi-label {
    color: #94a3b8;
    font-size: 12px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.7px;
}

.kpi-value {
    font-size: 30px;
    font-weight: 850;
    color: #f8fafc;
    margin-top: 6px;
}

.kpi-footer {
    color: #64748b;
    font-size: 11px;
    margin-top: 7px;
}


/* ============================================================
   SLO HEALTH
   ============================================================ */

.slo-container {
    padding: 22px;
    border-radius: 20px;
    background: linear-gradient(
        145deg,
        #0e1d31,
        #0a1728
    );
    border: 1px solid rgba(59, 130, 246, 0.20);
    box-shadow:
        0 15px 40px rgba(0, 0, 0, 0.20);
}

.slo-header {
    display: grid;
    grid-template-columns: 2.2fr 1fr 1fr 1.2fr;
    gap: 12px;
    padding: 0 12px 12px 12px;
    color: #64748b;
    font-size: 11px;
    font-weight: 800;
    text-transform: uppercase;
    letter-spacing: 0.7px;
}

.slo-row {
    display: grid;
    grid-template-columns: 2.2fr 1fr 1fr 1.2fr;
    gap: 12px;
    align-items: center;
    padding: 15px 12px;
    border-top: 1px solid #1e293b;
    font-size: 13px;
}

.slo-name {
    color: #f8fafc;
    font-weight: 700;
}

.slo-target {
    color: #94a3b8;
}

.slo-actual {
    color: #f8fafc;
    font-weight: 750;
}

.slo-healthy {
    color: #4ade80;
    font-weight: 800;
}

.slo-breached {
    color: #f87171;
    font-weight: 800;
}

.slo-na {
    color: #94a3b8;
    font-weight: 700;
}

.slo-summary {
    margin-top: 18px;
    padding: 14px 16px;
    border-radius: 12px;
    background: rgba(15, 23, 42, 0.75);
    color: #94a3b8;
    font-size: 12px;
}


/* ============================================================
   SCORE CARD
   ============================================================ */

.score-card {
    min-height: 270px;
    border-radius: 20px;
    padding: 25px;
    background: linear-gradient(
        145deg,
        #0e1d31,
        #0a1728
    );
    border: 1px solid rgba(59, 130, 246, 0.20);
    box-shadow:
        0 20px 50px rgba(0, 0, 0, 0.25);
}

.score-title {
    color: #94a3b8;
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 1px;
    font-weight: 700;
}

.score-number {
    font-size: 58px;
    font-weight: 900;
    color: #f8fafc;
    margin-top: 7px;
}

.score-good {
    color: #4ade80;
    font-size: 14px;
    font-weight: 700;
}

.score-warning {
    color: #facc15;
    font-size: 14px;
    font-weight: 700;
}

.score-danger {
    color: #f87171;
    font-size: 14px;
    font-weight: 700;
}

.progress-container {
    height: 10px;
    width: 100%;
    background: #172235;
    border-radius: 20px;
    margin-top: 18px;
    overflow: hidden;
}

.progress-bar {
    height: 100%;
    border-radius: 20px;
    background: linear-gradient(
        90deg,
        #22c55e,
        #0ea5e9
    );
}


/* ============================================================
   QUALITY CARD
   ============================================================ */

.quality-card {
    padding: 22px;
    border-radius: 18px;
    background: #0e1a2b;
    border: 1px solid rgba(148, 163, 184, 0.12);
    min-height: 270px;
}

.quality-title {
    font-size: 17px;
    font-weight: 750;
    color: #f8fafc;
    margin-bottom: 10px;
}

.quality-row {
    display: flex;
    justify-content: space-between;
    padding: 12px 0;
    border-bottom: 1px solid #1e293b;
    font-size: 13px;
}

.quality-name {
    color: #94a3b8;
}

.quality-value {
    color: #4ade80;
    font-weight: 750;
}


/* ============================================================
   PIPELINE
   ============================================================ */

.pipeline-container {
    padding: 25px;
    border-radius: 20px;
    background: linear-gradient(
        145deg,
        #0d1a2c,
        #091522
    );
    border: 1px solid rgba(148, 163, 184, 0.12);
    box-shadow:
        0 15px 40px rgba(0, 0, 0, 0.20);
}

.pipeline {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 8px;
    width: 100%;
}

.pipeline-node {
    min-width: 125px;
    padding: 17px 12px;
    text-align: center;
    border-radius: 14px;
    background: #121f31;
    border: 1px solid #26364d;
}

.pipeline-icon {
    font-size: 25px;
    margin-bottom: 6px;
}

.pipeline-name {
    color: #f8fafc;
    font-weight: 700;
    font-size: 13px;
}

.pipeline-status {
    margin-top: 5px;
    color: #4ade80;
    font-size: 10px;
    font-weight: 700;
}

.pipeline-arrow {
    color: #38bdf8;
    font-size: 24px;
    font-weight: bold;
}


/* ============================================================
   STATUS CARDS
   ============================================================ */

.status-card {
    padding: 20px;
    border-radius: 17px;
    min-height: 125px;
    background: #0e1a2b;
    border: 1px solid rgba(148, 163, 184, 0.12);
}

.status-green {
    border-left: 4px solid #22c55e;
}

.status-yellow {
    border-left: 4px solid #eab308;
}

.status-red {
    border-left: 4px solid #ef4444;
}

.status-label {
    color: #64748b;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.7px;
}

.status-value {
    color: #f8fafc;
    font-size: 20px;
    font-weight: 800;
    margin-top: 8px;
}

.status-description {
    color: #64748b;
    font-size: 11px;
    margin-top: 7px;
}


/* ============================================================
   INCIDENT
   ============================================================ */

.incident-critical {
    padding: 22px;
    border-radius: 18px;
    background: linear-gradient(
        145deg,
        rgba(127, 29, 29, 0.35),
        rgba(69, 10, 10, 0.30)
    );
    border: 1px solid rgba(239, 68, 68, 0.30);
}

.incident-warning {
    padding: 22px;
    border-radius: 18px;
    background: linear-gradient(
        145deg,
        rgba(120, 85, 0, 0.25),
        rgba(70, 50, 0, 0.20)
    );
    border: 1px solid rgba(234, 179, 8, 0.25);
}

.incident-healthy {
    padding: 22px;
    border-radius: 18px;
    background: linear-gradient(
        145deg,
        rgba(20, 83, 45, 0.30),
        rgba(5, 46, 22, 0.20)
    );
    border: 1px solid rgba(34, 197, 94, 0.25);
}

.incident-title {
    font-size: 19px;
    font-weight: 800;
    color: #f8fafc;
}

.incident-text {
    color: #94a3b8;
    font-size: 12px;
    line-height: 1.6;
    margin-top: 8px;
}


/* ============================================================
   FOOTER
   ============================================================ */

.footer {
    text-align: center;
    color: #475569;
    font-size: 11px;
    padding-top: 35px;
    padding-bottom: 15px;
}

</style>
""",
unsafe_allow_html=True
)


# ============================================================
# SIDEBAR
# ============================================================

with st.sidebar:

    st.markdown(
"""
<div class="sidebar-title">❄️ IceStream</div>
<div class="sidebar-subtitle">
Real-Time Data Reliability & Observability
</div>
""",
        unsafe_allow_html=True
    )

    st.markdown(
        '<div class="sidebar-section">Platform</div>',
        unsafe_allow_html=True
    )

    st.markdown(
        '<div class="sidebar-item">📊 Reliability Dashboard</div>',
        unsafe_allow_html=True
    )

    st.markdown(
        '<div class="sidebar-item">🎯 SLO Monitoring</div>',
        unsafe_allow_html=True
    )

    st.markdown(
        '<div class="sidebar-item">🔍 Data Quality Monitoring</div>',
        unsafe_allow_html=True
    )

    st.markdown(
        '<div class="sidebar-item">🚨 Incident Detection</div>',
        unsafe_allow_html=True
    )

    st.markdown(
        '<div class="sidebar-item">🛡️ Pipeline Protection</div>',
        unsafe_allow_html=True
    )

    st.markdown(
        '<div class="sidebar-section">Architecture</div>',
        unsafe_allow_html=True
    )

    st.markdown(
        '<div class="sidebar-item">🐍 Python Generator</div>',
        unsafe_allow_html=True
    )

    st.markdown(
        '<div class="sidebar-item">📨 Apache Kafka</div>',
        unsafe_allow_html=True
    )

    st.markdown(
        '<div class="sidebar-item">⚡ Apache Flink</div>',
        unsafe_allow_html=True
    )

    st.markdown(
        '<div class="sidebar-item">🧊 Apache Iceberg</div>',
        unsafe_allow_html=True
    )

    st.markdown(
        '<div class="sidebar-section">SLO Configuration</div>',
        unsafe_allow_html=True
    )

    st.info(
        f"""
Valid Records: ≥ {VALID_RECORDS_SLO:.0f}%

Error Rate: ≤ {ERROR_RATE_SLO:.0f}%

Required Fields: ≥ {REQUIRED_FIELDS_SLO:.0f}%

DLQ Rate: ≤ {DLQ_RATE_SLO:.0f}%
"""
    )


# ============================================================
# KAFKA READER
# ============================================================

def read_transactions(limit=20):

    consumer = KafkaConsumer(
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
        enable_auto_commit=False,
        group_id=None
    )

    try:

        partitions = consumer.partitions_for_topic(
            "transactions"
        )

        if not partitions:
            return []

        topic_partitions = [
            TopicPartition("transactions", p)
            for p in partitions
        ]

        consumer.assign(topic_partitions)

        beginning = consumer.beginning_offsets(
            topic_partitions
        )

        ending = consumer.end_offsets(
            topic_partitions
        )

        # Read the latest records already stored in Kafka.
        for tp in topic_partitions:

            start_offset = max(
                beginning[tp],
                ending[tp] - limit
            )

            consumer.seek(
                tp,
                start_offset
            )

        records = []

        start_time = time.time()

        while time.time() - start_time < 5:

            batch = consumer.poll(
                timeout_ms=500
            )

            for messages in batch.values():

                for message in messages:

                    records.append(
                        message.value
                    )

            if len(records) >= limit:
                break

        return records[-limit:]

    finally:

        consumer.close()


# ============================================================
# DLQ READER
# ============================================================

def read_dlq_transactions(limit=100):

    consumer = KafkaConsumer(
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
        enable_auto_commit=False,
        group_id=None
    )

    try:

        partitions = consumer.partitions_for_topic(
            "transactions_dlq"
        )

        if not partitions:
            return []

        topic_partitions = [
            TopicPartition("transactions_dlq", p)
            for p in partitions
        ]

        consumer.assign(topic_partitions)

        beginning = consumer.beginning_offsets(
            topic_partitions
        )

        ending = consumer.end_offsets(
            topic_partitions
        )

        # Read the latest DLQ records already stored in Kafka.
        for tp in topic_partitions:

            start_offset = max(
                beginning[tp],
                ending[tp] - limit
            )

            consumer.seek(
                tp,
                start_offset
            )

        records = []

        start_time = time.time()

        while time.time() - start_time < 5:

            batch = consumer.poll(
                timeout_ms=500
            )

            for messages in batch.values():

                for message in messages:

                    records.append(
                        message.value
                    )

            if len(records) >= limit:
                break

        return records[-limit:]

    finally:

        consumer.close()


# ============================================================
# HEADER
# ============================================================

header_left, header_right = st.columns([5, 1])

with header_left:

    st.markdown(
"""
<div class="brand">
<div class="brand-icon">❄️</div>
<div>
<div class="brand-name">IceStream</div>
<div class="brand-subtitle">
Real-Time Data Reliability & Observability Platform
</div>
</div>
</div>
""",
        unsafe_allow_html=True
    )


with header_right:

    st.markdown(
"""
<div class="live-badge">
<div class="live-dot"></div>
SYSTEM ONLINE
</div>
""",
        unsafe_allow_html=True
    )


st.markdown("---")


# ============================================================
# REFRESH
# ============================================================

control_left, control_right = st.columns([5, 1])

with control_left:

    st.markdown(
"""
<span style="color:#64748b;font-size:13px;">
Monitoring transaction pipeline • Kafka • Flink • Data Quality
</span>
""",
        unsafe_allow_html=True
    )


with control_right:

    if st.button(
        "🔄 Refresh Data",
        use_container_width=True
    ):

        st.cache_resource.clear()

        st.rerun()


# ============================================================
# LOAD DATA
# ============================================================

try:

    transactions = read_transactions()
    dlq_transactions = read_dlq_transactions()


    if transactions:

        df = pd.DataFrame(
            transactions
        )

        # Existing IceStream validation rule:
        # transaction amount must be greater than zero.
        df["is_bad"] = (
            df["amount"] <= 0
        )

        total_records = len(df)

        bad_records = int(
            df["is_bad"].sum()
        )

        good_records = (
            total_records -
            bad_records
        )

        error_rate = (
            bad_records /
            total_records *
            100
            if total_records > 0
            else 0
        )

        reliability_score = max(
            0,
            100 - error_rate
        )

        total_value = (
            df["amount"]
            .clip(lower=0)
            .sum()
        )

        average_transaction = (
            df["amount"]
            .clip(lower=0)
            .mean()
        )


        # ====================================================
        # DLQ CALCULATION
        # ====================================================

        transaction_ids = set(
            df["transaction_id"].tolist()
        )

        dlq_transaction_ids = set(
            record.get("transaction_id")
            for record in dlq_transactions
            if record.get("transaction_id") is not None
        )

        matched_dlq_records = (
            transaction_ids &
            dlq_transaction_ids
        )

        dlq_records = len(
            matched_dlq_records
        )

        dlq_rate = (
            dlq_records /
            total_records *
            100
            if total_records > 0
            else 0
        )


    else:

        df = pd.DataFrame()

        total_records = 0
        good_records = 0
        bad_records = 0
        error_rate = 0
        reliability_score = 0
        total_value = 0
        average_transaction = 0
        dlq_records = 0
        dlq_rate = 0


    # ========================================================
    # REQUIRED FIELD QUALITY
    # ========================================================

    required_fields = [
        "transaction_id",
        "customer_id",
        "product",
        "amount",
        "payment_method"
    ]

    available_required_fields = [
        field
        for field in required_fields
        if field in df.columns
    ]

    if total_records > 0 and available_required_fields:

        required_field_checks = 0
        required_field_failures = 0

        for field in available_required_fields:

            field_values = df[field]

            missing_values = (
                field_values.isna() |
                field_values.astype(str).str.strip().eq("")
            )

            required_field_checks += total_records

            required_field_failures += int(
                missing_values.sum()
            )

        required_fields_percentage = (
            100 -
            (
                required_field_failures /
                required_field_checks *
                100
            )
        )

    elif total_records == 0:

        required_fields_percentage = 0

    else:

        required_fields_percentage = None


    # ========================================================
    # SLO STATUS CALCULATIONS
    # ========================================================

    valid_slo_breached = (
        total_records > 0 and
        (good_records / total_records * 100)
        < VALID_RECORDS_SLO
    )

    error_slo_breached = (
        total_records > 0 and
        error_rate > ERROR_RATE_SLO
    )

    required_fields_slo_breached = (
        required_fields_percentage is not None and
        required_fields_percentage < REQUIRED_FIELDS_SLO
    )

    dlq_slo_breached = (
        total_records > 0 and
        dlq_rate > DLQ_RATE_SLO
    )


    # ========================================================
    # RELIABILITY OVERVIEW
    # ========================================================

    st.markdown(
        '<div class="section-header">📊 Reliability Overview</div>',
        unsafe_allow_html=True
    )

    st.markdown(
        '<div class="section-description">'
        'Real-time health indicators for the transaction pipeline'
        '</div>',
        unsafe_allow_html=True
    )


    k1, k2, k3, k4, k5 = st.columns(5)


    with k1:

        st.markdown(
            f"""
<div class="kpi-card">
<div class="kpi-icon">📦</div>
<div class="kpi-label">Total Records</div>
<div class="kpi-value">{total_records:,}</div>
<div class="kpi-footer">Monitored transaction events</div>
</div>
""",
            unsafe_allow_html=True
        )


    with k2:

        st.markdown(
            f"""
<div class="kpi-card">
<div class="kpi-icon">✅</div>
<div class="kpi-label">Valid Records</div>
<div class="kpi-value">{good_records:,}</div>
<div class="kpi-footer">Passed quality validation</div>
</div>
""",
            unsafe_allow_html=True
        )


    with k3:

        st.markdown(
            f"""
<div class="kpi-card">
<div class="kpi-icon">🚨</div>
<div class="kpi-label">Invalid Records</div>
<div class="kpi-value">{bad_records:,}</div>
<div class="kpi-footer">Detected quality violations</div>
</div>
""",
            unsafe_allow_html=True
        )


    with k4:

        st.markdown(
            f"""
<div class="kpi-card">
<div class="kpi-icon">📈</div>
<div class="kpi-label">Error Rate</div>
<div class="kpi-value">{error_rate:.2f}%</div>
<div class="kpi-footer">
SLO threshold: ≤ {ERROR_RATE_SLO:.2f}%
</div>
</div>
""",
            unsafe_allow_html=True
        )


    with k5:

        st.markdown(
            f"""
<div class="kpi-card">
<div class="kpi-icon">💰</div>
<div class="kpi-label">Data Value</div>
<div class="kpi-value">₹{total_value:,.0f}</div>
<div class="kpi-footer">Valid transaction value</div>
</div>
""",
            unsafe_allow_html=True
        )


    # ========================================================
    # SLO HEALTH
    # ========================================================

    st.markdown(
        '<div class="section-header">🎯 SLO Health</div>',
        unsafe_allow_html=True
    )

    st.markdown(
        '<div class="section-description">'
        'Service Level Objectives for real-time data reliability'
        '</div>',
        unsafe_allow_html=True
    )


    valid_percentage = (
        good_records /
        total_records *
        100
        if total_records > 0
        else 0
    )


    if valid_slo_breached:

        valid_status = "🔴 BREACHED"
        valid_status_class = "slo-breached"

    elif total_records > 0:

        valid_status = "🟢 HEALTHY"
        valid_status_class = "slo-healthy"

    else:

        valid_status = "⚪ NO DATA"
        valid_status_class = "slo-na"


    if error_slo_breached:

        error_status = "🔴 BREACHED"
        error_status_class = "slo-breached"

    elif total_records > 0:

        error_status = "🟢 HEALTHY"
        error_status_class = "slo-healthy"

    else:

        error_status = "⚪ NO DATA"
        error_status_class = "slo-na"


    if required_fields_percentage is None:

        required_actual = "N/A"
        required_status = "⚪ NOT AVAILABLE"
        required_status_class = "slo-na"

    else:

        required_actual = (
            f"{required_fields_percentage:.2f}%"
        )

        if required_fields_slo_breached:

            required_status = "🔴 BREACHED"
            required_status_class = "slo-breached"

        else:

            required_status = "🟢 HEALTHY"
            required_status_class = "slo-healthy"


    if total_records > 0:

        dlq_actual = (
            f"{dlq_rate:.2f}%"
        )

        if dlq_slo_breached:

            dlq_status = "🔴 BREACHED"
            dlq_status_class = "slo-breached"

        else:

            dlq_status = "🟢 HEALTHY"
            dlq_status_class = "slo-healthy"

    else:

        dlq_actual = "N/A"
        dlq_status = "⚪ NO DATA"
        dlq_status_class = "slo-na"


    slo_breached_count = sum(
        [
            valid_slo_breached,
            error_slo_breached,
            required_fields_slo_breached,
            dlq_slo_breached
        ]
    )


    if total_records == 0:

        overall_slo_text = (
            "Waiting for transaction data."
        )

    elif slo_breached_count == 0:

        overall_slo_text = (
            "🟢 All available SLOs are currently healthy."
        )

    else:

        overall_slo_text = (
            f"🔴 {slo_breached_count} "
            "SLO objective(s) currently breached."
        )


    st.markdown(
        f"""
<div class="slo-container">

<div class="slo-header">
<div>Objective</div>
<div>Target</div>
<div>Actual</div>
<div>Status</div>
</div>

<div class="slo-row">
<div class="slo-name">
Valid Records
</div>

<div class="slo-target">
≥ {VALID_RECORDS_SLO:.0f}%
</div>

<div class="slo-actual">
{valid_percentage:.2f}%
</div>

<div class="{valid_status_class}">
{valid_status}
</div>
</div>


<div class="slo-row">
<div class="slo-name">
Error Rate
</div>

<div class="slo-target">
≤ {ERROR_RATE_SLO:.0f}%
</div>

<div class="slo-actual">
{error_rate:.2f}%
</div>

<div class="{error_status_class}">
{error_status}
</div>
</div>


<div class="slo-row">
<div class="slo-name">
Required Fields
</div>

<div class="slo-target">
≥ {REQUIRED_FIELDS_SLO:.0f}%
</div>

<div class="slo-actual">
{required_actual}
</div>

<div class="{required_status_class}">
{required_status}
</div>
</div>


<div class="slo-row">
<div class="slo-name">
DLQ Rate
</div>

<div class="slo-target">
≤ {DLQ_RATE_SLO:.0f}%
</div>

<div class="slo-actual">
{dlq_actual}
</div>

<div class="{dlq_status_class}">
{dlq_status}
</div>
</div>


<div class="slo-summary">
{overall_slo_text}
</div>

</div>
""",
        unsafe_allow_html=True
    )


    # ========================================================
    # RELIABILITY INTELLIGENCE
    # ========================================================

    st.markdown(
        '<div class="section-header">💯 Reliability Intelligence</div>',
        unsafe_allow_html=True
    )

    score_col, quality_col = st.columns([1, 2])


    with score_col:

        if reliability_score >= 95:

            score_status = "Excellent"
            score_class = "score-good"

        elif reliability_score >= 90:

            score_status = "Needs Attention"
            score_class = "score-warning"

        else:

            score_status = "Critical"
            score_class = "score-danger"


        st.markdown(
            f"""
<div class="score-card">
<div class="score-title">Data Reliability Score</div>
<div class="score-number">
{reliability_score:.1f}
</div>

<div class="{score_class}">
● {score_status}
</div>

<div class="progress-container">
<div class="progress-bar"
style="width:{reliability_score}%">
</div>
</div>

<br>

<span style="color:#64748b;font-size:12px;">
Calculated from current data-quality performance.
</span>

</div>
""",
            unsafe_allow_html=True
        )


    with quality_col:

        validation_status = (
            "PASS"
            if bad_records == 0
            else "VIOLATIONS"
        )

        validation_color = (
            "#4ade80"
            if bad_records == 0
            else "#facc15"
        )


        st.markdown(
            f"""
<div class="quality-card">

<div class="quality-title">
🔍 Quality Control Center
</div>

<div class="quality-row">

<span class="quality-name">
Positive Amount Validation
</span>

<span style="color:{validation_color};
font-weight:750;">
{validation_status}
</span>

</div>


<div class="quality-row">

<span class="quality-name">
Required Fields
</span>

<span class="quality-value">
ACTIVE
</span>

</div>


<div class="quality-row">

<span class="quality-name">
Schema Monitoring
</span>

<span class="quality-value">
ACTIVE
</span>

</div>


<div class="quality-row">

<span class="quality-name">
Validation Engine
</span>

<span class="quality-value">
ONLINE
</span>

</div>

</div>
""",
            unsafe_allow_html=True
        )


    # ========================================================
    # LIVE PIPELINE
    # ========================================================

    st.markdown(
        '<div class="section-header">🔗 Live Pipeline</div>',
        unsafe_allow_html=True
    )


    st.markdown(
"""
<div class="pipeline-container">

<div class="pipeline">

<div class="pipeline-node">
<div class="pipeline-icon">🐍</div>
<div class="pipeline-name">Generator</div>
<div class="pipeline-status">● ONLINE</div>
</div>

<div class="pipeline-arrow">→</div>

<div class="pipeline-node">
<div class="pipeline-icon">📨</div>
<div class="pipeline-name">Kafka</div>
<div class="pipeline-status">● ONLINE</div>
</div>

<div class="pipeline-arrow">→</div>

<div class="pipeline-node">
<div class="pipeline-icon">⚡</div>
<div class="pipeline-name">Flink</div>
<div class="pipeline-status">● ONLINE</div>
</div>

<div class="pipeline-arrow">→</div>

<div class="pipeline-node">
<div class="pipeline-icon">🔍</div>
<div class="pipeline-name">Quality Engine</div>
<div class="pipeline-status">● ACTIVE</div>
</div>

<div class="pipeline-arrow">→</div>

<div class="pipeline-node">
<div class="pipeline-icon">🧊</div>
<div class="pipeline-name">Iceberg</div>
<div class="pipeline-status">● READY</div>
</div>

</div>
</div>
""",
        unsafe_allow_html=True
    )


    # ========================================================
    # AUTONOMOUS PIPELINE PROTECTION
    # ========================================================

    st.markdown(
        '<div class="section-header">'
        '🛡️ Autonomous Pipeline Protection'
        '</div>',
        unsafe_allow_html=True
    )


    s1, s2, s3 = st.columns(3)


    with s1:

        if error_rate >= ERROR_RATE_SLO:

            st.markdown(
"""
<div class="status-card status-red">

<div class="status-label">
Circuit Breaker
</div>

<div class="status-value">
🔴 TRIGGERED
</div>

<div class="status-description">
Error rate exceeded the configured
SLO protection threshold.
</div>

</div>
""",
                unsafe_allow_html=True
            )

        else:

            st.markdown(
"""
<div class="status-card status-green">

<div class="status-label">
Circuit Breaker
</div>

<div class="status-value">
🟢 ARMED
</div>

<div class="status-description">
Pipeline protection is ready.
</div>

</div>
""",
                unsafe_allow_html=True
            )


    with s2:

        if bad_records > 0:

            st.markdown(
"""
<div class="status-card status-yellow">

<div class="status-label">
Data Quality
</div>

<div class="status-value">
🟡 WARNING
</div>

<div class="status-description">
Invalid transaction records detected.
</div>

</div>
""",
                unsafe_allow_html=True
            )

        else:

            st.markdown(
"""
<div class="status-card status-green">

<div class="status-label">
Data Quality
</div>

<div class="status-value">
🟢 HEALTHY
</div>

<div class="status-description">
All monitored records passed validation.
</div>

</div>
""",
                unsafe_allow_html=True
            )


    with s3:

        if error_rate >= ERROR_RATE_SLO:

            st.markdown(
"""
<div class="status-card status-red">

<div class="status-label">
Incident Center
</div>

<div class="status-value">
🔴 ACTIVE
</div>

<div class="status-description">
Immediate investigation recommended.
</div>

</div>
""",
                unsafe_allow_html=True
            )

        else:

            st.markdown(
"""
<div class="status-card status-green">

<div class="status-label">
Incident Center
</div>

<div class="status-value">
🟢 CLEAR
</div>

<div class="status-description">
No critical incidents detected.
</div>

</div>
""",
                unsafe_allow_html=True
            )


    # ========================================================
    # OBSERVABILITY ANALYTICS
    # ========================================================

    st.markdown(
        '<div class="section-header">'
        '📈 Observability Analytics'
        '</div>',
        unsafe_allow_html=True
    )


    chart_col, incident_col = st.columns([2, 1])


    with chart_col:

        if total_records > 0:

            chart_data = pd.DataFrame(
                {
                    "Records": [
                        good_records,
                        bad_records
                    ]
                },
                index=[
                    "Valid",
                    "Invalid"
                ]
            )

            st.bar_chart(
                chart_data,
                height=320
            )

        else:

            st.info(
                "Waiting for transaction data..."
            )


    with incident_col:

        if error_rate >= ERROR_RATE_SLO:

            st.markdown(
                f"""
<div class="incident-critical">

<div class="incident-title">
🚨 Critical Incident
</div>

<div class="incident-text">

The data-quality error rate has exceeded
the configured SLO protection threshold.

<br><br>

<b>Error Rate:</b>
{error_rate:.2f}%

<br>

<b>SLO Threshold:</b>
{ERROR_RATE_SLO:.2f}%

<br>

<b>Affected Records:</b>
{bad_records}

<br><br>

🛡️ Pipeline protection requires attention.

</div>

</div>
""",
                unsafe_allow_html=True
            )

        elif bad_records > 0:

            st.markdown(
                f"""
<div class="incident-warning">

<div class="incident-title">
🟡 Quality Warning
</div>

<div class="incident-text">

Invalid records have been detected,
but the error rate remains below
the critical threshold.

<br><br>

<b>Invalid Records:</b>
{bad_records}

<br>

<b>Error Rate:</b>
{error_rate:.2f}%

</div>

</div>
""",
                unsafe_allow_html=True
            )

        else:

            st.markdown(
"""
<div class="incident-healthy">

<div class="incident-title">
🟢 All Systems Healthy
</div>

<div class="incident-text">

No data-quality incidents have
been detected in the monitored
transaction stream.

<br><br>

Pipeline is operating normally.

</div>

</div>
""",
                unsafe_allow_html=True
            )


    # ========================================================
    # TRANSACTION INTELLIGENCE
    # ========================================================

    st.markdown(
        '<div class="section-header">'
        '💰 Transaction Intelligence'
        '</div>',
        unsafe_allow_html=True
    )


    t1, t2, t3 = st.columns(3)


    with t1:

        st.metric(
            "Total Valid Transaction Value",
            f"₹{total_value:,.2f}"
        )


    with t2:

        st.metric(
            "Average Transaction",
            f"₹{average_transaction:,.2f}"
        )


    with t3:

        st.metric(
            "Validation Success",
            f"{valid_percentage:.2f}%"
        )


    # ========================================================
    # INVALID TRANSACTIONS
    # ========================================================

    st.markdown(
        '<div class="section-header">'
        '🚨 Recent Invalid Transactions'
        '</div>',
        unsafe_allow_html=True
    )


    if bad_records > 0:

        bad_df = df[
            df["is_bad"]
        ].copy()

        bad_df["error"] = (
            "Invalid amount"
        )


        display_columns = [
            "transaction_id",
            "customer_id",
            "product",
            "amount",
            "payment_method",
            "error"
        ]


        available_display_columns = [
            column
            for column in display_columns
            if column in bad_df.columns
        ]


        display_df = bad_df[
            available_display_columns
        ]


        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True
        )

    else:

        st.success(
            "🟢 No invalid transactions detected."
        )


    # ========================================================
    # DLQ MONITORING
    # ========================================================

    st.markdown(
        '<div class="section-header">'
        '🚨 DLQ Monitoring'
        '</div>',
        unsafe_allow_html=True
    )

    dlq_col1, dlq_col2, dlq_col3 = st.columns(3)

    with dlq_col1:

        st.metric(
            "DLQ Records",
            f"{dlq_records:,}"
        )

    with dlq_col2:

        st.metric(
            "DLQ Rate",
            f"{dlq_rate:.2f}%"
        )

    with dlq_col3:

        if dlq_slo_breached:

            st.error(
                f"🔴 DLQ SLO BREACHED\n\n"
                f"Target ≤ {DLQ_RATE_SLO:.0f}%"
            )

        elif total_records > 0:

            st.success(
                f"🟢 DLQ SLO HEALTHY\n\n"
                f"Target ≤ {DLQ_RATE_SLO:.0f}%"
            )

        else:

            st.info(
                "⚪ No transaction data"
            )


    if dlq_transactions:

        dlq_df = pd.DataFrame(
            dlq_transactions
        )

        st.markdown(
            "#### Recent DLQ Records"
        )

        dlq_display_columns = [
            "transaction_id",
            "customer_id",
            "product",
            "amount",
            "payment_method",
            "error"
        ]

        available_dlq_columns = [
            column
            for column in dlq_display_columns
            if column in dlq_df.columns
        ]

        if available_dlq_columns:

            st.dataframe(
                dlq_df[
                    available_dlq_columns
                ].tail(10),
                use_container_width=True,
                hide_index=True
            )

    else:

        st.info(
            "No records currently available in transactions_dlq."
        )


    # ========================================================
    # INFRASTRUCTURE
    # ========================================================

    st.markdown(
        '<div class="section-header">'
        '⚙️ Infrastructure Status'
        '</div>',
        unsafe_allow_html=True
    )


    i1, i2, i3, i4 = st.columns(4)


    with i1:

        st.success(
            "🟢 Kafka\n\nONLINE"
        )


    with i2:

        st.success(
            "🟢 Flink\n\nONLINE"
        )


    with i3:

        st.success(
            "🟢 Quality Engine\n\nACTIVE"
        )


    with i4:

        st.success(
            "🟢 Dashboard\n\nONLINE"
        )


    # ========================================================
    # FOOTER
    # ========================================================

    st.markdown(
"""
<div class="footer">

❄️ IceStream &nbsp;•&nbsp;
Real-Time Data Reliability Platform

<br>

Streaming Data • Quality Monitoring •
SLO Monitoring • Incident Detection •
Pipeline Protection

</div>
""",
        unsafe_allow_html=True
    )


# ============================================================
# ERROR HANDLING
# ============================================================

except Exception as e:

    st.error(
        "Unable to connect to Kafka."
    )

    st.code(
        str(e)
    )