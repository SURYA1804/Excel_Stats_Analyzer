import pandas as pd
from typing import Dict, List, Tuple
from collections import defaultdict, Counter
import io
from typing import List  # Added missing import

def load_multiple_excels(uploaded_files: List) -> Dict[str, pd.DataFrame]:
    """Load all Excel files into dict of DataFrames with file_sheet naming"""
    dfs = {}
    for file in uploaded_files:
        try:
            with pd.ExcelFile(io.BytesIO(file.read())) as xls:
                for sheet in xls.sheet_names:
                    df = pd.read_excel(file, sheet_name=sheet)
                    # Clean filename for key
                    filename = file.name.replace('.xlsx', '').replace('.xls', '')
                    key = f"{filename}_{sheet}"
                    dfs[key] = df
        except Exception as e:
            print(f"Error loading {file.name}: {e}")
    return dfs


def find_auto_join_columns(dfs: Dict[str, pd.DataFrame]) -> Dict[str, List[str]]:
    """Find common columns across DataFrames for auto-joining"""
    all_columns = defaultdict(list)
    for name, df in dfs.items():
        for col in df.columns:
            all_columns[str(col).lower()].append(name)
    
    # Columns appearing in 2+ DataFrames
    joinable = {col: names for col, names in all_columns.items() if len(names) > 1}
    return joinable


def smart_join_dfs(main_df: pd.DataFrame, other_dfs: Dict, join_cols: List[str]) -> pd.DataFrame:
    """**LEFT JOIN** other DataFrames to main on first matching column (KEEPS ALL MAIN DATA)"""
    merged = main_df.copy()
    
    for name, df in other_dfs.items():
        joined = False
        for col in join_cols:
            if col in merged.columns and col in df.columns:
                # 🔄 CHANGED: Always use LEFT JOIN to preserve main_df rows
                merged = pd.merge(merged, df, on=col, how='left', suffixes=('', f'_{name}'))
                print(f"✅ LEFT JOINED {name} on column '{col}'")
                print(f"   📊 Merged shape: {merged.shape}")
                joined = True
                break
        if not joined:
            print(f"⚠️  No common column found for {name} - SKIPPED")
    
    return merged


def get_df_summary(df: pd.DataFrame) -> str:
    """Get concise DataFrame summary for LLM context"""
    return f"""Shape: {df.shape}
Columns: {list(df.columns)}
Sample:
{df.head(3).to_string()}"""


def find_common_cols(df_left: "pd.DataFrame", df_right: "pd.DataFrame") -> list:
    """Return list of column names present in both DataFrames."""
    import pandas as pd
    left_cols  = set(df_left.columns.str.strip().str.lower())
    right_cols = set(df_right.columns.str.strip().str.lower())
    common_lower = left_cols & right_cols
    # Return original-case names from the left df
    return [c for c in df_left.columns if c.strip().lower() in common_lower]


def do_join(df_left: "pd.DataFrame", df_right: "pd.DataFrame", on: str) -> "pd.DataFrame":
    """
    Left-join df_left with df_right on the given column.
    Handles duplicate column names by suffixing with _left / _right.
    """
    import pandas as pd
    result = pd.merge(df_left, df_right, on=on, how="left", suffixes=("_left", "_right"))
    return result