import pandas as pd
from langchain_google_genai import ChatGoogleGenerativeAI
import io
import json
import os
import re
import time
from dotenv import load_dotenv
from uuid import uuid4
from datetime import datetime
from app.services.ddl_generator import sanitize_column_name


# Load environment variables
load_dotenv()

# Store last uploaded tables and history by target
PREVIOUS_TABLES = {
    "databricks": {},
    "snowflake": {}
}
TABLE_HISTORY = {
    "databricks": [],
    "snowflake": []
}

def get_llm():
    """Initialize Gemini LLM instance with API key."""
    google_api_key = os.getenv("GOOGLE_API_KEY")
    if not google_api_key:
        raise ValueError("GOOGLE_API_KEY not found in environment variables.")
    return ChatGoogleGenerativeAI(
        model="gemini-2.5-flash-preview-05-20",
        temperature=0.1,
        api_key=google_api_key
    )

def parse_file(file_bytes: bytes, filename: str = None) -> pd.DataFrame:
    """Read Excel or CSV file into pandas DataFrame."""
    try:
        if filename and filename.lower().endswith(".csv"):
            return pd.read_csv(io.BytesIO(file_bytes))
        elif filename and (filename.lower().endswith(".xls") or filename.lower().endswith(".xlsx")):
            return pd.read_excel(io.BytesIO(file_bytes), engine="openpyxl")
        else:
            # Try Excel first, fallback to CSV
            try:
                return pd.read_excel(io.BytesIO(file_bytes), engine="openpyxl")
            except Exception:
                return pd.read_csv(io.BytesIO(file_bytes))
    except Exception as e:
        raise ValueError(f"Failed to parse file: {str(e)}")

def sanitize_for_json(obj):
    """Replace NaN/Inf with None for JSON compatibility."""
    if isinstance(obj, pd.DataFrame):
        return obj.where(pd.notnull(obj), None)
    elif isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_json(v) for v in obj]
    elif pd.isna(obj) or obj in [float('inf'), float('-inf')]:
        return None
    else:
        return obj

def clean_llm_output(result: str) -> str:
    """Remove Markdown-style ```json ... ``` wrappers."""
    if result.startswith("```"):
        result = re.sub(r"^```[a-zA-Z]*\n", "", result)
        result = re.sub(r"\n```$", "", result)
    return result.strip()

def generate_full_ddl(df: pd.DataFrame, table_name: str, target: str) -> str:
    """Use LLM to generate full CREATE TABLE DDL including all rows."""
    # Sanitize column names
    df = df.rename(columns={c: sanitize_column_name(c) for c in df.columns})
    
    # Convert full dataset to JSON-friendly dict
    all_data = sanitize_for_json(df.replace({pd.NA: None}).to_dict(orient="records"))
    
    llm = get_llm()
    
    prompt = f"""
You are a professional database engineer.
Given the following table data:
{all_data}

1. Use the sanitized column names already provided (no spaces or special chars).
2. Generate a CREATE TABLE statement for {target} named {table_name}.
3. 3. Generate a single INSERT INTO statement with multiple VALUES tuples
   instead of separate INSERTs per row.

Return JSON:
{{
"ddl": "<DDL with sample data>"
}}
Only return JSON, no commentary.
"""
    result = llm.predict(prompt)
    cleaned_result = clean_llm_output(result)
    try:
        return json.loads(cleaned_result)["ddl"]
    except Exception as e:
        raise ValueError(f"Failed to parse JSON from LLM output: {repr(cleaned_result)}") from e



def generate_change_log(new_df: pd.DataFrame, table_name: str, target: str) -> dict:
    """Compare with previous table to generate INSERT/UPDATE/DELETE statements."""
    global PREVIOUS_TABLES
    previous_df = PREVIOUS_TABLES[target].get(table_name, pd.DataFrame())

    if previous_df.empty:
        inserts = sanitize_for_json(new_df.to_dict(orient="records"))
        updates = []
        deletes = []
    else:
        merged = new_df.merge(previous_df, indicator=True, how='outer')
        inserts = sanitize_for_json(
            merged[merged['_merge'] == 'left_only'].drop(columns=['_merge']).to_dict(orient="records")
        )
        deletes = sanitize_for_json(
            merged[merged['_merge'] == 'right_only'].drop(columns=['_merge']).to_dict(orient="records")
        )

        # Updates
        common_cols = new_df.columns.intersection(previous_df.columns)
        updates_df = pd.concat([new_df[common_cols], previous_df[common_cols]], axis=1, keys=['new', 'old'])
        updates = []
        for idx, row in updates_df.iterrows():
            new_row = row['new']
            old_row = row['old']
            if not new_row.equals(old_row):
                updates.append(sanitize_for_json({'old': old_row.to_dict(), 'new': new_row.to_dict()}))

    PREVIOUS_TABLES[target][table_name] = new_df.copy()
    return {
        "inserts": inserts,
        "updates": updates,
        "deletes": deletes
    }

def record_table_metadata(table_name: str, target: str, row_count: int, processing_time: float, batch_id: str) -> list:
    """Record table metadata in global history by target."""
    global TABLE_HISTORY
    entry = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "table_name": table_name,
        "target": target,
        "batch_id": batch_id,
        "rows_processed": row_count,
        "processing_time": f"{processing_time:.2f}s"
    }
    TABLE_HISTORY[target].append(entry)
    return TABLE_HISTORY[target]

def analyze_and_generate_ddl_with_changes(df: pd.DataFrame, table_name: str, target: str) -> dict:
    """Generate full DDL and dynamic transaction log with metadata for frontend."""
    start_time = time.time()
    full_ddl = generate_full_ddl(df, table_name, target)
    change_log = generate_change_log(df, table_name, target)
    processing_time = time.time() - start_time
    batch_id = str(uuid4())[:8]

    history = record_table_metadata(table_name, target, len(df), processing_time, batch_id)

    return {
        "ddl": full_ddl,
        "changes": change_log,
        "history": history
    }

def invoke(state: dict) -> dict:
    """
    Entry point for FastAPI route.
    Expects state dict with keys:
    - 'file': bytes of uploaded file
    - 'filename': original filename
    - 'target': 'databricks' or 'snowflake'
    - 'table_name': optional table name
    """
    df = parse_file(state["file"], state.get("filename"))
    table_name = state.get("table_name", "uploaded_table")
    target = state["target"]
    return analyze_and_generate_ddl_with_changes(df, table_name, target)