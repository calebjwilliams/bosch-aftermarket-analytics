# Bosch Aftermarket Analytics

An end-to-end analytics and AI automation project built in response to a technical interview task. The project consists of two components: a Tableau dashboard analyzing vehicle parc and workshop distribution, and a natural language querying application powered by a local LLM pipeline.

## Live Dashboard

[View Tableau Dashboard](https://calebjwilliams.github.io/bosch-aftermarket-analytics/)

## Project Overview

**Task:** Design and build an AI automation flow that uses data and a local LLM to generate insights or actions for a vehicle aftermarket business.

**Approach:** A Streamlit web application that allows users to ask plain-language questions about vehicle population data. Questions are processed through a multi-step LLM pipeline that generates and executes DuckDB SQL queries, then returns business insights synthesized by a reasoning model.

## Architecture

The pipeline uses two specialized local models running via Ollama, rather than a single general-purpose model:

1. **User inputs a natural language question** via the Streamlit UI
2. **Reasoning model (Llama 3.1)** generates a structured SQL plan — specifying columns, filters, and aggregations
3. **SQL model (Gemma 2)** converts the plan into valid DuckDB SQL
4. **DuckDB** executes the query against the vehicle dataset
5. **Reasoning model (Llama 3.1)** synthesizes the results into executive-level business insights

## Design Decisions

**Why two models instead of one?**
Reasoning tasks (interpreting a question, identifying relevant filters, generating business insights) and code generation tasks (writing syntactically correct, constraint-compliant SQL) benefit from different prompting strategies and temperature settings. A single model tends to trade off between natural language reasoning quality and code generation precision. Routing each task to a purpose-appropriate model improved output reliability.

**Why local models?**
Hardware constraints — limited RAM and CPU — ruled out larger models. Llama 3.1 and Gemma 2 were selected as the best-performing models available within these constraints, with Gemma 2 chosen specifically for its strong SQL generation relative to its size.

**Why not RAG?**
Retrieval-Augmented Generation is designed for unstructured document corpora where relevant context needs to be retrieved before generation. This project operates on a single structured tabular dataset with a known schema — a scenario better suited to direct SQL querying. RAG would add unnecessary architectural complexity without meaningful benefit at this data scale.

**Why DuckDB?**
DuckDB offers fast in-process analytical querying on CSV files without requiring a database server, making it well-suited for local development with tabular data.

## Tools & Technologies

- **Python** — pandas, requests
- **Ollama** — local LLM inference (Llama 3.1, Gemma 2)
- **DuckDB** — in-process SQL query engine
- **Streamlit** — web application frontend
- **Tableau** — vehicle parc and workshop distribution dashboard

## Note on Data

The source dataset is not included in this repository due to file size constraints and data confidentiality. To run the application, provide a compatible CSV and update `DATA_PATH` in `pipeline.py` accordingly.
