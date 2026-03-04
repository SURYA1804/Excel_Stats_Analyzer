"""
graph.py — DataLens LangGraph Pipelines
========================================

TWO COMPILED GRAPHS:

1. PlainTextGraph  (Agent 1)
   ┌─────────┐     ┌───────┐
   │  agent  │────▶│ tools │
   └─────────┘◀────└───────┘
       │ (no more tool calls)
       ▼
      END
   • Uses bind_tools + SQL tool (ReAct loop)
   • Returns a plain-text "Final Answer:" string
   • Used directly by analyze_query()

2. StructuredGraph  (Agent 2)
   ┌──────────────┐
   │ generate_sql │  Node 1 — LLM writes SQL (no tools, plain invoke)
   └──────┬───────┘
     sql_ok│  sql_error
           │         ╲
   ┌───────▼──────┐  ┌▼──────────┐
   │ execute_sql  │  │ fallback  │  Node 4 — delegates to PlainTextGraph
   └──────┬───────┘  └─────┬─────┘
    ok    │  empty  error  │
          │    ╲      ╱    │
   ┌──────▼──┐  ▼    ╱   END
   │generate │ ┌▼────────┐
   │ _meta   │ │assemble │  Node 5 — builds final dict from pandas DataFrame
   └─────┬───┘ └────┬────┘
         └────┬─────┘
         ┌────▼────┐
         │assemble │
         └────┬────┘
             END

   • Node 1 + Node 3 use plain llm.invoke() — NO tools bound
     (Groq raises 400 BadRequestError when tools are bound and the model
      outputs JSON, because it tries to parse it as a tool call)
   • Node 2 runs SQL via pandas.read_sql_query() — handles any row count
   • Node 3 receives ONLY col names + row count + 3-row sample — never full rows
   • All actual row data flows through pandas, never through the LLM
"""

import os
import json
import logging
import sqlite3
import operator
from typing import TypedDict, Annotated, List, Dict, Any, Optional

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from langchain_groq import ChatGroq
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage, SystemMessage
from langchain_community.utilities import SQLDatabase
from langchain_community.tools.sql_database.tool import QuerySQLDatabaseTool
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode, tools_condition

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("excel_analyzer")


# ══════════════════════════════════════════════════════════════════════════════
# STATE DEFINITIONS
# ══════════════════════════════════════════════════════════════════════════════

class PlainTextState(TypedDict):
    """State for the ReAct plain-text agent (Agent 1)."""
    messages:   Annotated[List[BaseMessage], operator.add]
    df_summary: str
    merged_df:  pd.DataFrame


class StructuredState(TypedDict):
    """
    State for the structured pipeline (Agent 2).
    Every field is populated by exactly one node; all start as None.
    """
    # ── Inputs (set at graph entry, never mutated) ─────────────────────────
    question:   str
    df_summary: str
    col_names:  str         # comma-separated column names
    raw_conn:   Any         # shared sqlite3.Connection
    llm:        Any         # ChatGroq instance

    # ── Node 1: generate_sql ───────────────────────────────────────────────
    generated_sql: Optional[str]
    sql_error:     Optional[str]

    # ── Node 2: execute_sql ────────────────────────────────────────────────
    result_df:       Optional[Any]   # pd.DataFrame
    execution_error: Optional[str]

    # ── Node 3: generate_meta ──────────────────────────────────────────────
    summary:    Optional[str]
    followups:  Optional[List[str]]
    meta_error: Optional[str]

    # ── Node 4: fallback ───────────────────────────────────────────────────
    fallback_text: Optional[str]

    # ── Node 5: assemble (also written by fallback) ────────────────────────
    final_result: Optional[Dict]


# ══════════════════════════════════════════════════════════════════════════════
# SHARED INFRASTRUCTURE
# ══════════════════════════════════════════════════════════════════════════════

def _build_db(df: pd.DataFrame):
    """
    Write df into a single sqlite3 :memory: connection and return it.

    CRITICAL — StaticPool pattern:
        SQLite :memory: databases are scoped to ONE connection.
        SQLAlchemy's default pool creates multiple connections, each seeing
        an empty database. StaticPool forces every request through the same
        raw_conn, so the 'data' table is always visible.
    """
    raw_conn = sqlite3.connect(":memory:", check_same_thread=False)
    df.to_sql("data", raw_conn, index=False, if_exists="replace")
    tables = raw_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    logger.info("SQLite | %d rows loaded | tables: %s", len(df), tables)

    engine = create_engine(
        "sqlite:///:memory:",
        creator=lambda: raw_conn,
        poolclass=StaticPool,
    )
    db = SQLDatabase(engine)
    logger.debug("Usable tables via SQLDatabase: %s", db.get_usable_table_names())
    return db, raw_conn


def _build_llm() -> ChatGroq:
    return ChatGroq(
        model=os.getenv("MODEL", "llama3-70b-8192"),
        temperature=0,
        api_key=os.getenv("GROQ_API_KEY"),
    )


def _extract_sql(text: str) -> str:
    """Strip markdown fences / whitespace / trailing semicolons from LLM SQL output."""
    sql = text.strip()
    if sql.startswith("```"):
        parts = sql.split("```")
        sql = parts[1] if len(parts) > 1 else sql
        if sql.lower().startswith("sql"):
            sql = sql[3:]
    return " ".join(l for l in sql.splitlines() if l.strip()).strip().rstrip(";")


def _parse_json_block(text: str) -> dict:
    """Extract and parse the first { … } JSON block from LLM output."""
    content = text.strip()
    if content.startswith("```"):
        parts = content.split("```")
        content = parts[1] if len(parts) > 1 else content
        if content.lower().startswith("json"):
            content = content[4:]
    start, end = content.find("{"), content.rfind("}")
    if start == -1 or end == -1:
        raise ValueError(f"No JSON block found in LLM output: {content[:200]}")
    return json.loads(content[start : end + 1])


# ══════════════════════════════════════════════════════════════════════════════
# AGENT 1 — PLAIN TEXT  (ReAct loop with SQL tool)
# ══════════════════════════════════════════════════════════════════════════════

_PLAIN_TEXT_SYSTEM = """You are an expert data analyst answering questions about \
Excel data stored in a SQL table called `data`.

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


def _build_plain_text_graph(df: pd.DataFrame, df_summary: str):
    """
    Compile Agent 1: a LangGraph ReAct loop.

    Graph:
        agent ──(tool call)──▶ tools
          ▲                      │
          └──────────────────────┘
          │ (no tool call)
          ▼
         END
    """
    llm      = _build_llm()
    db, _    = _build_db(df)
    sql_tool = QuerySQLDatabaseTool(db=db)
    system   = _PLAIN_TEXT_SYSTEM.format(df_summary=df_summary)

    def agent_node(state: PlainTextState) -> Dict[str, Any]:
        logger.debug("Agent1 | agent_node | history: %d msgs", len(state["messages"]))
        resp = llm.bind_tools([sql_tool]).invoke(
            [SystemMessage(content=system)] + state["messages"]
        )
        logger.debug("Agent1 | tool_calls=%s | content=%.150s",
                     [tc["name"] for tc in (resp.tool_calls or [])], resp.content)
        return {"messages": [resp]}

    wf = StateGraph(PlainTextState)
    wf.add_node("agent", agent_node)
    wf.add_node("tools", ToolNode([sql_tool]))
    wf.set_entry_point("agent")
    wf.add_conditional_edges("agent", tools_condition, {"tools": "tools", END: END})
    wf.add_edge("tools", "agent")
    return wf.compile()


def analyze_query(df: pd.DataFrame, query: str, df_summary: str) -> str:
    """Public API — run Agent 1, return the plain-text Final Answer string."""
    logger.info("analyze_query | %s", query)
    try:
        graph  = _build_plain_text_graph(df, df_summary)
        result = graph.invoke({
            "messages":   [HumanMessage(content=query)],
            "df_summary": df_summary,
            "merged_df":  df,
        })
        ai_msgs = [m for m in result["messages"] if isinstance(m, AIMessage)]
        for msg in reversed(ai_msgs):
            if "Final Answer:" in msg.content:
                answer = msg.content.split("Final Answer:", 1)[1].strip()
                logger.info("analyze_query | answer: %.150s", answer)
                return answer
        return ai_msgs[-1].content if ai_msgs else "No answer generated."
    except Exception as exc:
        logger.exception("analyze_query failed")
        return f"❌ Error: {exc}"


# ══════════════════════════════════════════════════════════════════════════════
# AGENT 2 — STRUCTURED  (5-node LangGraph with typed state + routing)
# ══════════════════════════════════════════════════════════════════════════════

# ── Prompts ────────────────────────────────────────────────────────────────────

_SQL_GEN_PROMPT = """\
You are a SQL expert. Write ONE valid SQLite SELECT query for the question below.

Table name  : data
Columns     : {col_names}

DATA SUMMARY:
{df_summary}

STRICT RULES:
- Output ONLY the raw SQL. No explanation, no markdown, no backticks.
- Use only column names listed above.
- Always: SELECT ... FROM data ...
- Top-N   → ORDER BY <col> DESC LIMIT N  (include all relevant columns)
- Aggregate → SELECT cat_col, AGG(val_col) FROM data GROUP BY cat_col
- Include the primary key,Naming Columns like Name,department columns wherever possible.

Question: {question}
SQL:"""

_META_PROMPT = """\
You are a data analyst. Output ONLY the JSON below — no markdown, no extra text.

{{
  "summary": "One clear sentence describing what the result shows.",
  "followups": [
    "Relevant follow-up question 1",
    "Relevant follow-up question 2",
    "Relevant follow-up question 3"
  ]
}}

Question       : {question}
Columns        : {col_names}
Total rows     : {row_count}
First 3 rows   : {sample}"""


# ── Node 1 — generate_sql ──────────────────────────────────────────────────────
def node_generate_sql(state: StructuredState) -> StructuredState:
    """
    Ask the LLM (plain invoke — NO tools bound) to produce a SQL SELECT query.

    Why no tools here?
        We only need a SQL string, not data yet.
        Using bind_tools would risk Groq treating the SQL string as a tool call.
    """
    logger.info("Node1 | generate_sql | q: %s", state["question"])
    try:
        prompt = _SQL_GEN_PROMPT.format(
            col_names=state["col_names"],
            df_summary=state["df_summary"],
            question=state["question"],
        )
        resp = state["llm"].invoke([HumanMessage(content=prompt)])
        sql  = _extract_sql(resp.content)
        logger.info("Node1 | generated SQL: %s", sql)

        if not sql.upper().lstrip().startswith("SELECT"):
            raise ValueError(f"Not a SELECT statement: {sql[:80]}")

        return {**state, "generated_sql": sql, "sql_error": None}

    except Exception as exc:
        logger.warning("Node1 | FAILED: %s", exc)
        return {**state, "generated_sql": None, "sql_error": str(exc)}


# ── Node 2 — execute_sql ───────────────────────────────────────────────────────
def node_execute_sql(state: StructuredState) -> StructuredState:
    """
    Execute the SQL via pandas.read_sql_query() on the shared sqlite3 connection.

    Why pandas, not the LLM SQL tool?
        The SQL tool returns a text string truncated to fit the context window.
        pandas.read_sql_query() returns a full DataFrame — 10 rows or 10 M rows,
        same memory, zero token cost.
    """
    logger.info("Node2 | execute_sql | sql: %s", state["generated_sql"])
    try:
        df = pd.read_sql_query(state["generated_sql"], state["raw_conn"])
        logger.info("Node2 | OK | shape: %s", df.shape)
        return {**state, "result_df": df, "execution_error": None}

    except Exception as exc:
        logger.warning("Node2 | FAILED: %s", exc)
        return {**state, "result_df": None, "execution_error": str(exc)}


# ── Node 3 — generate_meta ─────────────────────────────────────────────────────
def node_generate_meta(state: StructuredState) -> StructuredState:
    """
    Ask the LLM (plain invoke — NO tools) for a summary sentence + 3 follow-ups.

    What we send to the LLM:
        • column names        (a few words)
        • total row count     (one integer)
        • first 3 rows only   (tiny sample — structure hint, not data dump)

    What we do NOT send:
        • all rows            (could be MB of text → context overflow)
        • the raw SQL result  (not needed — LLM only writes prose/JSON)
    """
    df = state["result_df"]
    logger.info("Node3 | generate_meta | rows: %d", len(df))
    try:
        prompt = _META_PROMPT.format(
            question=state["question"],
            col_names=df.columns.tolist(),
            row_count=len(df),
            sample=df.head(3).values.tolist(),
        )
        resp   = state["llm"].invoke([HumanMessage(content=prompt)])
        parsed = _parse_json_block(resp.content)
        logger.info("Node3 | OK | followups: %d", len(parsed.get("followups", [])))
        return {
            **state,
            "summary":    parsed.get("summary", f"Returned {len(df):,} rows."),
            "followups":  parsed.get("followups", [])[:3],
            "meta_error": None,
        }
    except Exception as exc:
        logger.warning("Node3 | meta parse failed: %s — using defaults", exc)
        return {
            **state,
            "summary":    f"Query returned {len(df):,} rows.",
            "followups":  [],
            "meta_error": str(exc),
        }


# ── Node 4 — fallback ──────────────────────────────────────────────────────────
def node_fallback(state: StructuredState) -> StructuredState:
    """
    Called when Node1 (SQL generation) OR Node2 (SQL execution) fails.
    Delegates to Agent 1 (plain-text ReAct graph) which uses the SQL tool
    internally and can handle any question robustly.
    Wraps the plain-text result into the standard final_result shape.
    """
    reason = state.get("sql_error") or state.get("execution_error") or "unknown"
    logger.info("Node4 | fallback | reason: %s", reason)
    try:
        full_df = pd.read_sql_query("SELECT * FROM data", state["raw_conn"])
        plain   = analyze_query(full_df, state["question"], state["df_summary"])
    except Exception as exc:
        logger.exception("Node4 | fallback also failed")
        plain = f"Could not answer the question. Error: {exc}"

    return {
        **state,
        "fallback_text": plain,
        "final_result": {
            "summary":   plain,
            "columns":   ["Answer"],
            "rows":      [[plain]],
            "followups": [],
            "error":     None,
        },
    }


# ── Node 5 — assemble ──────────────────────────────────────────────────────────
def node_assemble(state: StructuredState) -> StructuredState:
    """
    Build the final_result dict.
    Row data comes from the pandas DataFrame — the LLM never touched it.
    """
    logger.info("Node5 | assemble")
    df = state.get("result_df")

    if df is None or df.empty:
        final = {
            "summary":   state.get("summary") or "No results found.",
            "columns":   df.columns.tolist() if df is not None else [],
            "rows":      [],
            "followups": state.get("followups") or [],
            "error":     None,
        }
    else:
        final = {
            "summary":   state.get("summary", f"Returned {len(df):,} rows."),
            "columns":   df.columns.tolist(),
            "rows":      df.values.tolist(),   # complete dataset — no size limit
            "followups": state.get("followups") or [],
            "error":     None,
        }

    logger.info("Node5 | done | cols=%s rows=%d followups=%d",
                final["columns"], len(final["rows"]), len(final["followups"]))
    return {**state, "final_result": final}


# ── Routing functions ──────────────────────────────────────────────────────────

def _route_after_generate_sql(state: StructuredState) -> str:
    if state.get("sql_error"):
        logger.info("Router | generate_sql → fallback")
        return "fallback"
    return "execute_sql"


def _route_after_execute_sql(state: StructuredState) -> str:
    if state.get("execution_error"):
        logger.info("Router | execute_sql → fallback")
        return "fallback"
    df = state.get("result_df")
    if df is None or df.empty:
        logger.info("Router | execute_sql → assemble (empty result)")
        return "assemble"
    return "generate_meta"


# ── Compile Agent 2 ────────────────────────────────────────────────────────────

def _build_structured_graph():
    """
    Compile Agent 2.

    Node wiring:
        generate_sql ──ok──▶ execute_sql ──ok──▶ generate_meta ──▶ assemble ──▶ END
                     ╲error             ╲error                  ╱
                      ╲                  ╲──────▶ fallback ─────
                       ╲                 ╲empty▶ assemble
                        ╲──────────────────────▶ fallback
    """
    wf = StateGraph(StructuredState)

    wf.add_node("generate_sql",  node_generate_sql)
    wf.add_node("execute_sql",   node_execute_sql)
    wf.add_node("generate_meta", node_generate_meta)
    wf.add_node("fallback",      node_fallback)
    wf.add_node("assemble",      node_assemble)

    wf.set_entry_point("generate_sql")

    wf.add_conditional_edges(
        "generate_sql",
        _route_after_generate_sql,
        {"execute_sql": "execute_sql", "fallback": "fallback"},
    )
    wf.add_conditional_edges(
        "execute_sql",
        _route_after_execute_sql,
        {"generate_meta": "generate_meta", "assemble": "assemble", "fallback": "fallback"},
    )

    wf.add_edge("generate_meta", "assemble")
    wf.add_edge("assemble",      END)
    wf.add_edge("fallback",      END)

    return wf.compile()


# ── Public API ─────────────────────────────────────────────────────────────────

def analyze_query_structured(df: pd.DataFrame, query: str, df_summary: str) -> dict:
    """
    Public API — run Agent 2 (structured LangGraph).

    Returns:
        {
          "summary":   str,           one-sentence description
          "columns":   [str, ...],    actual column names from SQL result
          "rows":      [[val, ...]],  full data from pandas — no truncation
          "followups": [str, str, str],
          "error":     str | None
        }
    """
    logger.info("analyze_query_structured | query: %s", query)
    try:
        llm          = _build_llm()
        _, raw_conn  = _build_db(df)

        initial: StructuredState = {
            # inputs
            "question":        query,
            "df_summary":      df_summary,
            "col_names":       ", ".join(df.columns.tolist()),
            "raw_conn":        raw_conn,
            "llm":             llm,
            # node outputs — all None at graph entry
            "generated_sql":   None,
            "sql_error":       None,
            "result_df":       None,
            "execution_error": None,
            "summary":         None,
            "followups":       None,
            "meta_error":      None,
            "fallback_text":   None,
            "final_result":    None,
        }

        graph  = _build_structured_graph()
        result = graph.invoke(initial)

        final = result.get("final_result")
        if final:
            return final

        # Should never reach here — both assemble and fallback always set final_result
        return {"summary": "No result produced.", "columns": [], "rows": [], "followups": [], "error": None}

    except Exception as exc:
        logger.exception("analyze_query_structured outer exception")
        return {"summary": "", "columns": [], "rows": [], "followups": [], "error": str(exc)}
    



