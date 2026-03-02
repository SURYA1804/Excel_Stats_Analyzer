import os
import json
import logging
import sqlite3
import operator
from typing import TypedDict, Annotated, List, Dict, Any

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from langchain_groq import ChatGroq
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage, SystemMessage
from langchain_community.utilities import SQLDatabase
from langchain_community.tools.sql_database.tool import QuerySQLDatabaseTool
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode, tools_condition

# ── Console logging ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("excel_analyzer")


class AgentState(TypedDict):
    messages: Annotated[List[BaseMessage], operator.add]
    df_summary: str
    merged_df: pd.DataFrame


def _build_db(df: pd.DataFrame):
    """Load df into a single shared SQLite :memory: connection via StaticPool."""
    raw_conn = sqlite3.connect(":memory:", check_same_thread=False)
    df.to_sql("data", raw_conn, index=False, if_exists="replace")
    tables = raw_conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    logger.info("SQLite loaded | %d rows | tables: %s", len(df), tables)

    engine = create_engine(
        "sqlite:///:memory:",
        creator=lambda: raw_conn,
        poolclass=StaticPool,
    )
    db = SQLDatabase(engine)
    logger.debug("Usable tables: %s", db.get_usable_table_names())
    return db, raw_conn


def _build_llm():
    return ChatGroq(
        model=os.getenv("MODEL", "llama3-70b-8192"),
        temperature=0,
        api_key=os.getenv("GROQ_API_KEY"),
    )


# ══════════════════════════════════════════════════════════════════════════════
# 1.  PLAIN TEXT AGENT  (used for simple non-tabular answers)
# ══════════════════════════════════════════════════════════════════════════════

def create_excel_analyzer_graph(df: pd.DataFrame, df_summary: str):
    logger.info("Building text agent | shape: %s", df.shape)
    llm = _build_llm()
    db, _ = _build_db(df)
    sql_tool = QuerySQLDatabaseTool(db=db)

    system_prompt = f"""You are an expert data analyst answering questions about Excel data in a SQL table called `data`.

DATA SUMMARY:
{df_summary}

Rules:
- Use sql_db_query to run SELECT queries on `data`.
- Only read-only SELECT statements.
- Write "Final Answer:" exactly ONCE at the very end.
- Present multiple items as a numbered list with ALL relevant columns.
- Never compress a list into one sentence.

PATTERNS:
- Count:   SELECT COUNT(*) FROM data WHERE age >= 35
- Average: SELECT AVG(salary) FROM data GROUP BY department_name
- Top N:   SELECT * FROM data ORDER BY performance_score DESC LIMIT 5
"""

    def agent_node(state: AgentState) -> Dict[str, Any]:
        messages = state["messages"]
        logger.debug("text agent_node | history: %d msgs", len(messages))
        full = [SystemMessage(content=system_prompt)] + messages
        resp = llm.bind_tools([sql_tool]).invoke(full)
        logger.debug("LLM tool_calls=%s | content=%.200s",
                     [tc["name"] for tc in (resp.tool_calls or [])], resp.content)
        return {"messages": [resp]}

    wf = StateGraph(AgentState)
    wf.add_node("agent", agent_node)
    wf.add_node("tools", ToolNode([sql_tool]))
    wf.set_entry_point("agent")
    wf.add_conditional_edges("agent", tools_condition, {"tools": "tools", END: END})
    wf.add_edge("tools", "agent")
    return wf.compile()


def analyze_query(df: pd.DataFrame, query: str, df_summary: str) -> str:
    """Run query → return plain-text final answer."""
    logger.info("analyze_query | %s", query)
    try:
        app = create_excel_analyzer_graph(df, df_summary)
        result = app.invoke({
            "messages": [HumanMessage(content=query)],
            "df_summary": df_summary,
            "merged_df": df,
        })
        ai_msgs = [m for m in result["messages"] if isinstance(m, AIMessage)]
        for msg in reversed(ai_msgs):
            if "Final Answer:" in msg.content:
                final = msg.content.split("Final Answer:", 1)[1].strip()
                logger.info("Final answer: %.200s", final)
                return final
        if ai_msgs:
            return ai_msgs[-1].content
        return "No answer generated."
    except Exception as exc:
        logger.exception("analyze_query failed")
        return f"❌ Error: {exc}"



# ══════════════════════════════════════════════════════════════════════════════
# 2.  STRUCTURED AGENT — scale-safe two-step pipeline
# ══════════════════════════════════════════════════════════════════════════════
#
# PROBLEM WITH LARGE DATASETS (10,000+ rows):
#   Passing raw SQL result text to an LLM prompt does NOT scale.
#   10,000 rows × ~50 chars/row = ~500KB of text → context overflow / 413 error.
#
# SOLUTION — Never send row data to the LLM formatter:
#
#   Step 1 — SQL Extraction (LLM + tools):
#       Ask the LLM to generate ONLY the correct SQL query.
#       Run that SQL via pandas/sqlite directly — NOT through LLM text output.
#       This gives us a real pandas DataFrame regardless of row count.
#
#   Step 2 — JSON Metadata (LLM, no tools, tiny prompt):
#       Send the LLM only: the question + column names + row count + 3-row sample.
#       Ask it to generate: summary sentence + 3 follow-up questions.
#       The actual rows/columns come from the DataFrame — never from the LLM.
#
#   Result: works for 10 rows or 10,000,000 rows identically.

_SQL_GEN_PROMPT = """You are a SQL expert. Generate ONE valid SQLite SELECT query for the question below.

Table name: data
Available columns: {columns}

DATA SUMMARY:
{df_summary}

RULES:
- Output ONLY the raw SQL query. No explanation, no markdown, no backticks.
- Use only columns that exist in the table.
- Always use: SELECT ... FROM data ...
- For top-N: ORDER BY <col> DESC LIMIT N
- For aggregates: GROUP BY the category column
- Always include in Select Query:  Primary key from your knowledge and add relevant columns to query
Question: {question}
SQL:"""

_META_PROMPT = """You are a data analyst assistant. Given a question and a sample of query results, produce a JSON object.

Output ONLY this JSON, no markdown, no extra text:
{{
  "summary": "One clear sentence describing what the data shows",
  "followups": [
    "Specific follow-up question 1 based on this result",
    "Specific follow-up question 2 based on this result",
    "Specific follow-up question 3 based on this result"
  ]
}}

Question: {question}
Columns returned: {columns}
Total rows returned: {row_count}
Sample (first 3 rows): {sample}
"""


def _extract_sql(llm_response: str) -> str:
    """Clean up LLM SQL output — strip fences, whitespace, trailing semicolons."""
    sql = llm_response.strip()
    if sql.startswith("```"):
        parts = sql.split("```")
        sql = parts[1] if len(parts) > 1 else sql
        if sql.lower().startswith("sql"):
            sql = sql[3:]
        sql = sql.strip()
    # Keep only the SELECT statement
    lines = [l for l in sql.splitlines() if l.strip()]
    sql = " ".join(lines).strip().rstrip(";")
    return sql


def analyze_query_structured(df: pd.DataFrame, query: str, df_summary: str) -> dict:
    """
    Scale-safe structured pipeline.

    Step 1: LLM generates SQL → pandas executes it directly → DataFrame (any size).
    Step 2: LLM receives only column names + row count + 3-row sample → summary + followups.
    The actual rows/columns come from pandas, never from LLM text output.

    Returns:
        {
          "summary":   str,
          "columns":   [str, ...],
          "rows":      [[val, ...], ...],   # full result, no truncation
          "followups": [str, str, str],
          "error":     str | None
        }
    """
    logger.info("analyze_query_structured | query: %s", query)

    try:
        llm      = _build_llm()
        db, raw_conn = _build_db(df)
        col_names = ", ".join(df.columns.tolist())

        # ── Step 1a: Ask LLM to generate the SQL query only ──────────────────
        sql_prompt = _SQL_GEN_PROMPT.format(
            columns=col_names,
            df_summary=df_summary,
            question=query,
        )
        logger.debug("Step 1a: generating SQL for question")
        sql_response = llm.invoke([HumanMessage(content=sql_prompt)])
        generated_sql = _extract_sql(sql_response.content)
        logger.info("Step 1a SQL generated: %s", generated_sql)

        # ── Step 1b: Execute SQL directly via pandas — handles any row count ──
        try:
            result_df = pd.read_sql_query(generated_sql, raw_conn)
            logger.info("Step 1b SQL executed | shape: %s", result_df.shape)
        except Exception as sql_err:
            logger.warning("Step 1b SQL failed (%s) — retrying with plain text agent", sql_err)
            # Fallback: use the plain text agent if pandas execution fails
            plain = analyze_query(df, query, df_summary)
            return {
                "summary":   plain,
                "columns":   ["Answer"],
                "rows":      [[plain]],
                "followups": [],
                "error":     None,
            }

        if result_df.empty:
            return {
                "summary":   "No results found for this query.",
                "columns":   result_df.columns.tolist(),
                "rows":      [],
                "followups": [],
                "error":     None,
            }

        # ── Step 2: Ask LLM for summary + followups using only tiny metadata ──
        # We send: column names + total row count + 3-row sample only.
        # The actual data stays in the DataFrame — never in the prompt.
        result_cols   = result_df.columns.tolist()
        result_rows   = result_df.values.tolist()
        sample_rows   = result_df.head(3).values.tolist()
        sample_str    = str(sample_rows)

        meta_prompt = _META_PROMPT.format(
            question=query,
            columns=result_cols,
            row_count=len(result_rows),
            sample=sample_str,
        )
        logger.debug("Step 2: requesting summary + followups (tiny prompt)")
        meta_response = llm.invoke([HumanMessage(content=meta_prompt)])
        meta_content  = meta_response.content.strip()
        logger.debug("Step 2 meta response: %.400s", meta_content)

        # Parse JSON from meta response
        if meta_content.startswith("```"):
            parts = meta_content.split("```")
            meta_content = parts[1] if len(parts) > 1 else meta_content
            if meta_content.lower().startswith("json"):
                meta_content = meta_content[4:]
            meta_content = meta_content.strip()

        start = meta_content.find("{")
        end   = meta_content.rfind("}")
        meta_parsed = {}
        if start != -1 and end != -1:
            try:
                meta_parsed = json.loads(meta_content[start:end+1])
                logger.info("Step 2 meta parsed OK | followups: %d", len(meta_parsed.get("followups", [])))
            except json.JSONDecodeError:
                logger.warning("Step 2 meta JSON parse failed — using defaults")

        summary   = meta_parsed.get("summary", f"Query returned {len(result_rows)} rows.")
        followups = meta_parsed.get("followups", [])[:3]

        logger.info("Structured complete | cols=%s | total_rows=%d | followups=%d",
                    result_cols, len(result_rows), len(followups))

        return {
            "summary":   summary,
            "columns":   result_cols,
            "rows":      result_rows,   # full dataset from pandas — no size limit
            "followups": followups,
            "error":     None,
        }

    except Exception as exc:
        logger.exception("analyze_query_structured failed — falling back to plain text")
        try:
            plain = analyze_query(df, query, df_summary)
            return {
                "summary":   plain,
                "columns":   ["Answer"],
                "rows":      [[plain]],
                "followups": [],
                "error":     None,
            }
        except Exception:
            return {"summary": "", "columns": [], "rows": [], "followups": [], "error": str(exc)}