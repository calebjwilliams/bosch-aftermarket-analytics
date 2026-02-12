import numpy as np
import pandas as pd
import streamlit as st
import duckdb
import re
import requests
import time


# ============================
# Config
# ============================
SQL_MODEL = "gemma2:latest"
REASONING_MODEL = "llama3.1:latest"
DATA_PATH = "data/bosch_full.csv"

# ============================================================
# Load CSV into DuckDB (runs only once at import)
# ============================================================

duck = duckdb.connect("cars.duckdb")

duck.execute(
    """
    CREATE OR REPLACE TABLE cars AS
    SELECT * FROM read_csv_auto(?);
    """,
    [DATA_PATH],
)

schema_rows = duck.execute("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name='cars'
    ORDER BY ordinal_position
""").fetchall()

schema_text = "\n".join([f"{col} {dtype}" for col, dtype in schema_rows])

sample_rows = duck.execute("SELECT * FROM cars LIMIT 5").fetchdf()

sample_meta = duck.execute("""
    SELECT
      LIST(DISTINCT pop_vehicle_name) AS vehicle_names,
      LIST(DISTINCT pop_model_name) AS model_names,
      LIST(DISTINCT pop_fuel_type) AS fuel_types
    FROM cars;
""").fetchdf().iloc[0]

sample_text = f"""
    SAMPLE_ROWS:
    {sample_rows.to_string(index=False)}

    VEHICLE_NAMES:
    {', '.join(sample_meta['vehicle_names'])}

    MODEL_NAMES:
    {', '.join(sample_meta['model_names'])}

    FUEL_TYPES:
    {', '.join(sample_meta['fuel_types'])}
"""

duck.close()


# ============================================================
#  SQL Plan Generation 
# ============================================================

def make_sql_plan(question):

    PLAN_PROMPT = f"""
You must output exactly 4 lines. No explanations.

COLUMNS_NEEDED: <cols>
FILTERS: <filters or NONE>
AGGREGATION: <aggregation or NONE>
OUTPUT_SHAPE: <short description>

====================
STRICT FILTER RULES
====================

ABSOLUTE RULE:
- You may ONLY generate a filter if the literal value appears in the question text.
- NEVER output placeholders like <brand>, <model>, <year>, YYYY, XXXX.
- NEVER invent a brand, model, fuel type, or year.
- NEVER create new column names.
- ONLY use column names that appear in the schema.

====================
EXTRACT SAFETY RULE
====================
The ONLY allowed use of EXTRACT() is:
    EXTRACT(YEAR FROM pop_date)

You MUST NOT apply EXTRACT() to ANY other column.

====================
YEAR FILTER
====================
- Only add a year filter if the question contains a literal 4-digit year.
- If a year appears, output:
      EXTRACT(YEAR FROM pop_date) = <year>
- If no year appears, do NOT reference pop_date at all.

====================
AGE FILTER
====================
If question says:
- "older than X years" / "X years old" / "age > X":
      pop_avg_vehicle_age_years > X

====================
BRAND FILTER
====================
Only if exact brand appears:
      manufacturer = 'Brand'

====================
MODEL FILTER
====================
Only if exact model appears:
      pop_model_name = 'Model'

====================
GROUPING RULES
====================
- "Which city..." → group by city
- "Which country..." → group by country

====================
AGGREGATION RULES
====================
Population:
      SUM(pop_vehicle_population)
Retirement rate:
      SUM(pop_vehicle_retirements) / SUM(pop_vehicle_population)

====================
MANDATORY OUTPUT FORMAT
====================
COLUMNS_NEEDED: <comma-separated columns>
FILTERS: <filters joined with AND, or NONE>
AGGREGATION: <expression or NONE>
OUTPUT_SHAPE: <short description>

SCHEMA:
{schema_text}

QUESTION:
{question}
"""

    out = requests.post(
        "http://localhost:11434/api/generate",
        json={
            "model": REASONING_MODEL,
            "prompt": PLAN_PROMPT,
            "stream": False,
            "options": {"temperature": 0.0, "num_ctx": 4096}
        }
    )

    return out.json()["response"]


# ============================================================
# SQL Builder
# ============================================================

def build_sql_from_plan(plan):

    SQL_BUILDER_PROMPT = f"""
Convert PLAN into valid DuckDB SQL.

STRICT RULES:
- USE FILTERS EXACTLY AS WRITTEN.
- DO NOT add, modify, or remove filters.
- SELECT may include ONLY grouping columns + aggregates.
- SQL must SELECT FROM cars.
- No comments or markdown.

PLAN:
{plan}

Return only the SQL:
"""

    out = requests.post(
        "http://localhost:11434/api/generate",
        json={
            "model": SQL_MODEL,
            "prompt": SQL_BUILDER_PROMPT,
            "stream": False,
            "options": {"temperature": 0.0, "num_ctx": 4096}
        }
    )

    sql = out.json()["response"]
    return clean_sql(sql)


# ============================================================
# Utility Functions
# ============================================================

def clean_sql(sql):
    sql = re.sub(r"```.*?```", "", sql, flags=re.DOTALL)

    if ";" in sql:
        sql = sql.split(";")[0] + ";"

    sql = sql.strip()

    bad_line = re.compile(r"^[a-zA-Z0-9_]+(,\s*[a-zA-Z0-9_]+)*\s*$")
    good = []

    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.lower().startswith(("select", "from", "where", "group", "order", "limit")):
            good.append(line)
            continue
        if bad_line.match(stripped):
            continue
        good.append(line)

    sql = "\n".join(good)
    sql = re.sub(r"ORDER BY\s+ORDER BY", "ORDER BY", sql, flags=re.IGNORECASE)

    return sql.strip()


def make_sql(question):
    plan = make_sql_plan(question)
    return build_sql_from_plan(plan)


def run_sql(sql):
    with duckdb.connect("cars.duckdb") as conn:
        return conn.execute(sql).fetchall()


# ============================================================
# Insight Generation
# ============================================================

def ask_reasoning_llm(prompt):
    out = requests.post(
        "http://localhost:11434/api/generate",
        json={
            "model": REASONING_MODEL,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.3, "num_ctx": 4096}
        }
    )
    return out.json()["response"]


def make_insight(question, rows):
    INSIGHT_PROMPT = f"""
You are a senior automotive market strategist.

Your responsibilities:
- Answer the question directly.
- Use ONLY `rows` as numeric evidence.
- Identify the max/min strictly by numeric comparison.
- Never mention SQL or tuples.

Provide:
1. Direct answer.
2. Numeric statement.
3. 2 to 3 high-level business insights for:
   - OEMs
   - service workshops
   - dealerships
   - parts suppliers

User question:
{question}

Query results:
{rows}

Provide a clear executive explanation:
"""

    return ask_reasoning_llm(INSIGHT_PROMPT)



# ============================================================
# Debug runner (only executes when running this file directly)
# ============================================================

def run_debug_test():
    print("\n========================")
    print("DEBUG TEST MODE ACTIVE")
    print("========================\n")

    question = "Which city has the highest retirement rate among vehicles older than 10 years?"

    print("\n[1] Generating SQL PLAN...")
    plan = make_sql_plan(question)
    print(plan)

    print("\n[2] Building SQL from PLAN...")
    sql = build_sql_from_plan(plan)
    print(sql)

    print("\n[3] Running SQL...")
    rows = run_sql(sql)
    print("Rows:", rows)

    print("\n[4] Generating INSIGHT...")
    insight = make_insight(question, rows)
    print(insight)

    print("\n=== Debug run complete ===\n")


if __name__ == "__main__":
    run_debug_test()
