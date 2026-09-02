import streamlit as st


st.set_page_config(
    page_title="IceStream Observability",
    page_icon="❄️",
    layout="wide"
)


st.title("❄️ IceStream")
st.subheader("Real-Time Lakehouse Observability")


st.markdown("---")


col1, col2, col3, col4 = st.columns(4)


with col1:
    st.metric(
        label="Total Records",
        value="0"
    )


with col2:
    st.metric(
        label="Good Records",
        value="0"
    )


with col3:
    st.metric(
        label="Bad Records",
        value="0"
    )


with col4:
    st.metric(
        label="Error Rate",
        value="0%"
    )


st.markdown("---")


st.header("Pipeline")


st.write(
    """
    Python Generator → Kafka → Flink → Iceberg → Dashboard
    """
)


st.header("Data Quality")


st.info(
    "Waiting for streaming data..."
)