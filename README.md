<div align="center">

# 🔬 ExcelLens
### Excel Intelligence Platform

**Ask plain-English questions about your Excel data — powered by a conversational AI agent with memory**

[![Python](https://img.shields.io/badge/Python-3.12+-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.x-FF4B4B?style=flat&logo=streamlit&logoColor=white)](https://streamlit.io)
[![LangGraph](https://img.shields.io/badge/LangGraph-0.x-1C3A5E?style=flat&logo=langchain&logoColor=white)](https://langchain-ai.github.io/langgraph/)
[![Groq](https://img.shields.io/badge/Groq-LLaMA_3_70B-F55036?style=flat&logo=groq&logoColor=white)](https://groq.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-22c55e?style=flat)](LICENSE)

</div>

---

## What is ExcelLens?

ExcelLens turns your raw Excel files into a **conversational data analyst**. Upload multiple `.xlsx` files, auto-detect shared columns, build multi-level joins, and ask anything in plain English — including natural follow-up questions that reference previous answers.

Ask *"Show top 3 employees by salary"*, then simply ask *"What is their department?"* — ExcelLens remembers who "they" are, resolves the reference to their **primary keys**, and fetches the right data instantly.

It handles datasets of any size because the AI only **generates SQL** — pandas executes it directly, never passing row data through the LLM context window.

---

## ✨ Features

| Feature | Details |
|---|---|
| **Conversational Memory** | Resolves pronouns and back-references across turns using primary key tracking |
| **Multi-file Upload** | Load any number of `.xlsx` / `.xls` files at once |
| **Auto-join Detection** | Automatically finds shared columns across datasets |
| **Multi-level Joining** | Level 2 pre-joins for datasets not directly connected to main |
| **Plain-English Queries** | Ask statistical questions in natural language |
| **Structured Results** | Every answer renders as a dynamic, sortable table |
| **Scale-safe Pipeline** | Works with 100 rows or 10M rows — no context overflow |
| **Primary Key Filtering** | WHERE clauses always use primary keys, never name columns |
| **LLM Follow-ups** | 3 smart follow-up questions generated per answer |
| **One-click Excel Export** | Download results as a styled `.xlsx` with data + metadata sheets |
| **Dark UI** | Professional dark theme with IBM Plex + Syne typography |

---

## 🏗️ Architecture

ExcelLens is built around **two compiled LangGraph pipelines**:

### Agent 1 — Plain Text ReAct Loop

```
┌─────────┐     ┌───────┐
│  agent  │────▶│ tools │
└─────────┘◀────└───────┘
    │ (no more tool calls)
    ▼
   END
```

- Uses `bind_tools` with the SQL tool in a ReAct loop
- Returns a plain-text `Final Answer:` string
- Used as a robust fallback when structured queries fail

---

### Agent 2 — Structured 6-Node Pipeline with Conversational Memory

```
  chat_history ──▶ [ Node 0: reformulate_query ]  resolve pronouns + extract primary keys
                              │
                              ▼ self-contained question
                   [ Node 1: generate_sql     ]  LLM writes SQL — no tools, plain invoke
                              │
               ┌──────────────┴──────────────┐
           sql ok                         sql_error
               │                              │
               ▼                              ▼
  [ Node 2: execute_sql ]           [ Node 4: fallback ]  delegates to Agent 1
               │                              │
      ┌────────┼────────┐                    END
  exec ok   empty    exec_error
      │        │         │
      │        ▼         ▼
      │   [assemble]  [fallback]
      │
      ▼
  [ Node 3: generate_meta ]  summary + follow-ups — tiny prompt, 3-row sample only
              │
              ▼
  [ Node 5: assemble     ]  build final dict from full pandas DataFrame
              │
             END
```

| Node | Responsibility | LLM Call |
|------|---------------|----------|
| `node_reformulate_query` | Resolve pronouns / references using conversation history and primary keys | `llm.invoke()` — no tools |
| `node_generate_sql` | Write a primary-key-correct SELECT query | `llm.invoke()` — no tools |
| `node_execute_sql` | Run SQL via `pd.read_sql_query()` | None — pandas only |
| `node_generate_meta` | Write summary + 3 follow-up questions from 3-row sample | `llm.invoke()` — no tools |
| `node_fallback` | Delegate to Agent 1 on any failure | Agent 1 ReAct loop |
| `node_assemble` | Build the final result dict from the full DataFrame | None |

> **Why no `bind_tools` in Agent 2?**
> Groq raises a `400 BadRequestError` when tools are bound and the model outputs a plain string (SQL or JSON) — it tries to parse it as a tool call. All Agent 2 nodes use plain `llm.invoke()`.

> **Why pandas for SQL execution?**
> The LangChain SQL tool returns a truncated text string. `pd.read_sql_query()` returns a complete DataFrame — 100 rows or 10 million rows, same memory, zero token cost.

---

### 🧠 Conversational Memory — How It Works

ExcelLens tracks every Q&A turn and passes it to `node_reformulate_query` before any SQL is generated. This enables fully natural follow-up conversations:

```
Turn 1   User : "Show top 3 employees by salary"
         Agent: emp_id=[3, 7, 12] | names=[David Wilson, Mike Brown, Tom Lee]

Turn 2   User : "What is their department?"

         node_reformulate_query sees history:
           Turn 1 → emp_id values: [3, 7, 12] | names: David Wilson, Mike Brown, Tom Lee

         Rewrites to:
           "What is the department of employees with emp_id IN (3, 7, 12)?"

         node_generate_sql produces:
           SELECT emp_id, name, department FROM data WHERE emp_id IN (3, 7, 12)

Turn 3   User : "What is his mobile number?"  (referring to one person)
         Resolved → "What is the mobile number of the employee with emp_id = 3?"
```

**Primary key detection** — the reformulation node auto-detects the primary key from the previous result (prefers columns named `id` or ending in `_id`) and embeds those values into the reformulated question, so SQL generation always filters by key — never by name.

```
WRONG  : WHERE name IN ('David Wilson', 'Mike Brown')  ← fragile, case-sensitive
CORRECT: WHERE emp_id IN (3, 7)                        ← reliable, indexed, unambiguous
```

The UI also shows a subtle hint whenever a question gets reformulated:
```
🔄 Interpreted as: What is the department of employees with emp_id IN (3, 7, 12)?
```

---

### Multi-level Join System

```
Level 2 Pre-joins (optional):
  regions ──join── country_codes  →  regions_country_merged
  salary_bands ──join── tax_brackets  →  salary_tax_merged

Level 1 Main Join:
  employees (primary)
    ├── departments               (direct join)
    ├── regions_country_merged    (from L2)
    └── salary_tax_merged         (from L2)
         ↓
    MASTER DATASET  →  Conversational AI Query Interface
```

---

## 🚀 Getting Started

### Prerequisites

- Python 3.12+
- A [Groq API key](https://console.groq.com) (free tier available)

### Installation

```bash
# 1. Clone the repository
git clone https://github.com/SURYA1804/Excel_Stats_Analyzer.git
cd Excel_Stats_Analyzer

# 2. Create and activate a virtual environment
python -m venv my_venv

# Windows
my_venv\Scripts\activate
# macOS / Linux
source my_venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt
```

### Configuration

Create a `.env` file in the project root:

```env
GROQ_API_KEY=your_groq_api_key_here
MODEL=llama3-70b-8192
```

### Run

```bash
streamlit run app.py
```

Open [http://localhost:8501](http://localhost:8501) in your browser.

---

## 📁 Project Structure

```
ExcelLens/
├── app.py            # Streamlit UI — upload, join builder, chat interface
├── graph.py          # LangGraph pipelines — Agent 1 + Agent 2 with memory
├── utils.py          # Excel loading, join detection, multi-level join helpers
├── .env              # API keys (not committed)
├── requirements.txt
└── README.md
```

---

## 📦 Requirements

```txt
streamlit
pandas
openpyxl
python-dotenv
langchain-groq
langchain-community
langgraph
sqlalchemy
```

---

## 🔄 How It Works — Step by Step

### Step 1: Upload
Drop one or more `.xlsx` files into the sidebar. ExcelLens reads every sheet and detects column schemas automatically.

### Step 2: Join
**Level 2 (optional):** Pre-join datasets that share a column with each other but not with your main table. Each result becomes a named dataset available for the next step.

**Level 1:** Select your primary table and multiselect which datasets to join (including any L2 results). Auto-detected join keys are shown as live badges.

### Step 3: Query
Type any question — or a natural follow-up referencing a previous answer. The 6-node agent:

1. **Reformulates** — resolves pronouns and references using conversation history and primary key values
2. **Generates SQL** — writes a correct SELECT with primary key WHERE clauses
3. **Executes** — runs it via pandas against an in-memory SQLite database (any row count)
4. **Generates metadata** — writes a one-sentence summary and 3 follow-up questions from a 3-row sample
5. **Assembles** — builds the full result dict from the complete pandas DataFrame

Results render as a live sortable table. Click any follow-up chip to run it instantly.

### Step 4: Export
Click **⬇ Download Excel** to get a styled `.xlsx` with:
- **Sheet 1 "Results"** — full data table with actual column headers from the SQL result
- **Sheet 2 "Info"** — the original question and one-line summary

---

## 🖥️ Conversation Example

```
👤  Show top 3 employees by salary

🔬  The top 3 highest-paid employees are David Wilson, Mike Brown, and Tom Lee.

    emp_id │ name         │ department │ salary
    ───────┼──────────────┼────────────┼────────
    3      │ David Wilson │ Eng        │ 95,000
    7      │ Mike Brown   │ Sales      │ 88,000
    12     │ Tom Lee      │ HR         │ 82,000

    [⬇ Download Excel]
    Suggested: [Average salary by dept?] [Who ranks 4th?] [Filter by dept?]

──────────────────────────────────────────────────────────

👤  What is their department?

    🔄 Interpreted as: What is the department of employees with emp_id IN (3, 7, 12)?

🔬  All three employees work in different departments.

    emp_id │ name         │ department
    ───────┼──────────────┼────────────
    3      │ David Wilson │ Engineering
    7      │ Mike Brown   │ Sales
    12     │ Tom Lee      │ Human Resources

──────────────────────────────────────────────────────────

👤  What is his mobile number?

    🔄 Interpreted as: What is the mobile number of employee with emp_id = 3?

🔬  David Wilson's contact number is +91-9876543210.
```

---

## ⚙️ Configuration Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `GROQ_API_KEY` | — | Required. Get from [console.groq.com](https://console.groq.com) |
| `MODEL` | `llama3-70b-8192` | Groq model. Also supports `llama3-8b-8192`, `mixtral-8x7b-32768` |

---

## 🧠 Design Decisions

### Why primary keys instead of names in WHERE clauses?
Filtering by name (`WHERE name = 'David Wilson'`) is fragile — names can have inconsistent casing, trailing spaces, or duplicates across departments. Primary keys are unique, indexed, and unambiguous. The memory system extracts `emp_id` values from each result and passes them into reformulated questions so follow-up SQL is always `WHERE emp_id IN (3, 7)` — never `WHERE name IN ('David Wilson', 'Mike Brown')`.

### Why a dedicated reformulation node instead of prepending history to the SQL prompt?
Mixing language understanding and SQL generation into one prompt causes the model to trade off between resolving references and writing correct SQL. Separating them into two focused nodes — one that only thinks about language, one that only thinks about SQL — produces better output for both tasks.

### Why Groq?
Groq's inference speed makes the 6-node pipeline feel instantaneous — each LLM node completes in under a second, so the full reformulate → SQL → execute → meta → assemble chain finishes faster than a single slow LLM call elsewhere.

### Why SQLite in-memory with StaticPool?
SQLite `:memory:` databases are scoped to a single connection. SQLAlchemy's default pool creates multiple connections, each seeing an empty database. `StaticPool` forces all requests through one shared `raw_conn`, so the `data` table is always visible to every node.

### Why split Agent 2 into 6 nodes?
Each node has one responsibility, making failures trivial to trace in logs. The two router functions keep branching logic completely separate from business logic, and the fallback node guarantees the user always gets an answer — even when SQL generation or execution fails entirely.

---

## 🤝 Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you'd like to change.

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Commit your changes: `git commit -m 'Add your feature'`
4. Push to the branch: `git push origin feature/your-feature`
5. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

---

<div align="center">

Built with ❤️ using [LangGraph](https://langchain-ai.github.io/langgraph/) · [Groq](https://groq.com) · [Streamlit](https://streamlit.io) · [Pandas](https://pandas.pydata.org)

</div>