import streamlit as st
import time

# IMPORT PIPELINE
from pipeline import make_sql, run_sql, make_insight


st.set_page_config(
    page_title="Bosch Vehicle Insight Assistant",
    layout="wide"
)

# =============================
# HEADER
# =============================

st.title("Vehicle Insight Assistant")
st.write("Ask any question about vehicle populations, retirement rates, models, manufacturers, cities, etc!")
st.markdown("---")

# =============================
# USER INPUT
# =============================

question = st.text_input(
    "Enter your question:",
    placeholder="e.g. Which city has the highest retirement rate among vehicles older than 10 years?"
)

run_button = st.button("Run Query")


# =============================
# PIPELINE EXECUTION
# =============================

if run_button and question.strip():
    with st.spinner("Generating SQL plan..."):
        start = time.time()
        sql = make_sql(question)
        sql_time = time.time() - start

    st.subheader("Generated SQL")
    st.code(sql, language="sql")

    with st.spinner("Running SQL in DuckDB..."):
        start = time.time()
        rows = run_sql(sql)
        query_time = time.time() - start

    st.subheader("Query Results")
    if len(rows) == 0:
        st.warning("No rows returned from database.")
    else:
        st.dataframe(rows, use_container_width=True)

    with st.spinner("Generating Insight..."):
        start = time.time()
        insight = make_insight(question, rows)
        insight_time = time.time() - start

    st.subheader("Insight")
    st.write(insight)

    # =============================
    # TIMING INFO
    # =============================
    st.markdown("---")
    st.caption(f"SQL Generation Time: {sql_time:.3f}s")
    st.caption(f"SQL Execution Time: {query_time:.3f}s")
    st.caption(f"Insight Generation Time: {insight_time:.3f}s")



