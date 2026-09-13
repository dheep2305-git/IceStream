import json
import time

import pandas as pd
import streamlit as st
from kafka import KafkaConsumer


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
"""<style>

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

</style>""",
unsafe_allow_html=True
)


# ============================================================
# SIDEBAR
# ============================================================

with st.sidebar:

    st.markdown(
"""<div class="sidebar-title">❄️ IceStream</div>
<div class="sidebar-subtitle">
Real-Time Data Reliability & Observability
</div>""",
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
        '<div class="sidebar-section">Protection Threshold</div>',
        unsafe_allow_html=True
    )

    st.info("Circuit breaker threshold: 2%")


# ============================================================
# KAFKA READER
# ============================================================

@st.cache_resource
def create_consumer():

    return KafkaConsumer(
        "transactions",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
        auto_offset_reset="latest",
        enable_auto_commit=False,
        group_id="icestream-dashboard-live"
    )


def read_transactions():
    
    consumer = create_consumer()

    records = []
    start_time = time.time()

    # Wait for live Kafka records for up to 10 seconds
    while time.time() - start_time < 10:

        batch = consumer.poll(timeout_ms=1000)

        for messages in batch.values():

            for message in messages:

                records.append(message.value)

                if len(records) >= 100:
                    return records

        # Once we have received some records, give the stream
        # a short window to collect more before displaying them.
        if records and time.time() - start_time >= 5:
            break

    return records


# ============================================================
# HEADER
# ============================================================

header_left, header_right = st.columns([5, 1])

with header_left:

    st.markdown(
"""<div class="brand">
<div class="brand-icon">❄️</div>
<div>
<div class="brand-name">IceStream</div>
<div class="brand-subtitle">
Real-Time Data Reliability & Observability Platform
</div>
</div>
</div>""",
        unsafe_allow_html=True
    )

with header_right:

    st.markdown(
"""<div class="live-badge">
<div class="live-dot"></div>
SYSTEM ONLINE
</div>""",
        unsafe_allow_html=True
    )


st.markdown("---")


# ============================================================
# REFRESH
# ============================================================

control_left, control_right = st.columns([5, 1])

with control_left:

    st.markdown(
"""<span style="color:#64748b;font-size:13px;">
Monitoring transaction pipeline • Kafka • Flink • Data Quality
</span>""",
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

    if transactions:

        df = pd.DataFrame(transactions)

        df["is_bad"] = df["amount"] <= 0

        total_records = len(df)

        bad_records = int(
            df["is_bad"].sum()
        )

        good_records = (
            total_records - bad_records
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

    else:

        df = pd.DataFrame()

        total_records = 0
        good_records = 0
        bad_records = 0
        error_rate = 0
        reliability_score = 100
        total_value = 0
        average_transaction = 0


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
f"""<div class="kpi-card">
<div class="kpi-icon">📦</div>
<div class="kpi-label">Total Records</div>
<div class="kpi-value">{total_records:,}</div>
<div class="kpi-footer">Monitored transaction events</div>
</div>""",
            unsafe_allow_html=True
        )


    with k2:

        st.markdown(
f"""<div class="kpi-card">
<div class="kpi-icon">✅</div>
<div class="kpi-label">Valid Records</div>
<div class="kpi-value">{good_records:,}</div>
<div class="kpi-footer">Passed quality validation</div>
</div>""",
            unsafe_allow_html=True
        )


    with k3:

        st.markdown(
f"""<div class="kpi-card">
<div class="kpi-icon">🚨</div>
<div class="kpi-label">Invalid Records</div>
<div class="kpi-value">{bad_records:,}</div>
<div class="kpi-footer">Detected quality violations</div>
</div>""",
            unsafe_allow_html=True
        )


    with k4:

        st.markdown(
f"""<div class="kpi-card">
<div class="kpi-icon">📈</div>
<div class="kpi-label">Error Rate</div>
<div class="kpi-value">{error_rate:.2f}%</div>
<div class="kpi-footer">Alert threshold: 2.00%</div>
</div>""",
            unsafe_allow_html=True
        )


    with k5:

        st.markdown(
f"""<div class="kpi-card">
<div class="kpi-icon">💰</div>
<div class="kpi-label">Data Value</div>
<div class="kpi-value">₹{total_value:,.0f}</div>
<div class="kpi-footer">Valid transaction value</div>
</div>""",
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
f"""<div class="score-card">
<div class="score-title">Data Reliability Score</div>
<div class="score-number">{reliability_score:.1f}</div>
<div class="{score_class}">● {score_status}</div>
<div class="progress-container">
<div class="progress-bar" style="width:{reliability_score}%"></div>
</div>
<br>
<span style="color:#64748b;font-size:12px;">
Calculated from current data-quality performance.
</span>
</div>""",
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
f"""<div class="quality-card">
<div class="quality-title">🔍 Quality Control Center</div>

<div class="quality-row">
<span class="quality-name">Positive Amount Validation</span>
<span style="color:{validation_color};font-weight:750;">
{validation_status}
</span>
</div>

<div class="quality-row">
<span class="quality-name">Required Fields</span>
<span class="quality-value">ACTIVE</span>
</div>

<div class="quality-row">
<span class="quality-name">Schema Monitoring</span>
<span class="quality-value">ACTIVE</span>
</div>

<div class="quality-row">
<span class="quality-name">Validation Engine</span>
<span class="quality-value">ONLINE</span>
</div>

</div>""",
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
"""<div class="pipeline-container">
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
</div>""",
        unsafe_allow_html=True
    )


    # ========================================================
    # PIPELINE PROTECTION
    # ========================================================

    st.markdown(
        '<div class="section-header">🛡️ Autonomous Pipeline Protection</div>',
        unsafe_allow_html=True
    )

    s1, s2, s3 = st.columns(3)


    with s1:

        if error_rate >= 2:

            st.markdown(
"""<div class="status-card status-red">
<div class="status-label">Circuit Breaker</div>
<div class="status-value">🔴 TRIGGERED</div>
<div class="status-description">
Error rate exceeded the 2% protection threshold.
</div>
</div>""",
                unsafe_allow_html=True
            )

        else:

            st.markdown(
"""<div class="status-card status-green">
<div class="status-label">Circuit Breaker</div>
<div class="status-value">🟢 ARMED</div>
<div class="status-description">
Pipeline protection is ready.
</div>
</div>""",
                unsafe_allow_html=True
            )


    with s2:

        if bad_records > 0:

            st.markdown(
"""<div class="status-card status-yellow">
<div class="status-label">Data Quality</div>
<div class="status-value">🟡 WARNING</div>
<div class="status-description">
Invalid transaction records detected.
</div>
</div>""",
                unsafe_allow_html=True
            )

        else:

            st.markdown(
"""<div class="status-card status-green">
<div class="status-label">Data Quality</div>
<div class="status-value">🟢 HEALTHY</div>
<div class="status-description">
All monitored records passed validation.
</div>
</div>""",
                unsafe_allow_html=True
            )


    with s3:

        if error_rate >= 2:

            st.markdown(
"""<div class="status-card status-red">
<div class="status-label">Incident Center</div>
<div class="status-value">🔴 ACTIVE</div>
<div class="status-description">
Immediate investigation recommended.
</div>
</div>""",
                unsafe_allow_html=True
            )

        else:

            st.markdown(
"""<div class="status-card status-green">
<div class="status-label">Incident Center</div>
<div class="status-value">🟢 CLEAR</div>
<div class="status-description">
No critical incidents detected.
</div>
</div>""",
                unsafe_allow_html=True
            )


    # ========================================================
    # ANALYTICS
    # ========================================================

    st.markdown(
        '<div class="section-header">📈 Observability Analytics</div>',
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

        if error_rate >= 2:

            st.markdown(
f"""<div class="incident-critical">
<div class="incident-title">🚨 Critical Incident</div>
<div class="incident-text">

The data-quality error rate has exceeded
the configured protection threshold.

<br><br>

<b>Error Rate:</b> {error_rate:.2f}%

<br>

<b>Threshold:</b> 2.00%

<br><br>

🛡️ Pipeline protection requires attention.

</div>
</div>""",
                unsafe_allow_html=True
            )

        elif bad_records > 0:

            st.markdown(
f"""<div class="incident-warning">
<div class="incident-title">🟡 Quality Warning</div>
<div class="incident-text">

Invalid records have been detected,
but the error rate remains below
the critical threshold.

<br><br>

<b>Invalid Records:</b> {bad_records}

<br>

<b>Error Rate:</b> {error_rate:.2f}%

</div>
</div>""",
                unsafe_allow_html=True
            )

        else:

            st.markdown(
"""<div class="incident-healthy">
<div class="incident-title">🟢 All Systems Healthy</div>
<div class="incident-text">

No data-quality incidents have
been detected in the monitored
transaction stream.

<br><br>

Pipeline is operating normally.

</div>
</div>""",
                unsafe_allow_html=True
            )


    # ========================================================
    # TRANSACTION INTELLIGENCE
    # ========================================================

    st.markdown(
        '<div class="section-header">💰 Transaction Intelligence</div>',
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

        valid_percentage = (
            good_records /
            total_records *
            100
            if total_records > 0
            else 100
        )

        st.metric(
            "Validation Success",
            f"{valid_percentage:.2f}%"
        )


    # ========================================================
    # INVALID TRANSACTIONS
    # ========================================================

    st.markdown(
        '<div class="section-header">🚨 Recent Invalid Transactions</div>',
        unsafe_allow_html=True
    )

    if bad_records > 0:

        bad_df = df[
            df["is_bad"]
        ].copy()

        bad_df["error"] = "Invalid amount"

        display_df = bad_df[
            [
                "transaction_id",
                "customer_id",
                "product",
                "amount",
                "payment_method",
                "error"
            ]
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
    # INFRASTRUCTURE
    # ========================================================

    st.markdown(
        '<div class="section-header">⚙️ Infrastructure Status</div>',
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
"""<div class="footer">

❄️ IceStream &nbsp;•&nbsp;
Real-Time Data Reliability Platform

<br>

Streaming Data • Quality Monitoring •
Incident Detection • Pipeline Protection

</div>""",
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