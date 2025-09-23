# app/services/mapping_service.py
import pandas as pd
import io
import os
import json
import re
from uuid import uuid4
from datetime import datetime
from fastapi.responses import FileResponse
from app.agents.schema_agent import sanitize_for_json, clean_llm_output, get_llm

MAPPING_HISTORY = []
OUTPUT_DIR = "outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- Excel/CSV Parsing ---
def parse_excel(file_bytes: bytes, filename: str) -> pd.DataFrame:
    if filename.endswith(".xls"):
        return pd.read_excel(io.BytesIO(file_bytes), engine="xlrd")
    elif filename.endswith(".csv"):
        try:
            return pd.read_csv(io.BytesIO(file_bytes), encoding="utf-8")
        except UnicodeDecodeError:
            return pd.read_csv(io.BytesIO(file_bytes), encoding="latin1")
    else:
        return pd.read_excel(io.BytesIO(file_bytes), engine="openpyxl")

# --- Column Sanitization ---
def sanitize_column_name(col_name: str) -> str:
    sanitized = re.sub(r"[ ,;{}()\n\t=]", "_", col_name)
    sanitized = re.sub(r"__+", "_", sanitized)
    return sanitized.strip("_")

# --- Bronze → Silver Mapping ---
def map_bronze_to_silver(
    bronze_df: pd.DataFrame,
    silver_df: pd.DataFrame = None,   # optional
    bronze_name: str = "bronze_table",
    silver_name: str = "silver_table"
) -> dict:
    
    bronze_cols = list(bronze_df.columns)
    
    # --- Automatic Silver table creation if not provided ---
    if silver_df is None:
        # Auto-generate Silver columns: sanitize and infer types
        silver_cols = [sanitize_column_name(col) for col in bronze_cols]
        silver_df = pd.DataFrame(columns=silver_cols)
        
        # LLM suggestion: maybe add new columns
        llm = get_llm()
        prompt = f"""
You are a database assistant.
Given Bronze table columns: {bronze_cols} and sample data: {bronze_df.head(5).to_dict()},
suggest a Silver table schema:
- You can keep same columns or add new useful columns.
Return JSON array with:
  - silver_column
  - suggested (true if new column)
"""
        result = llm.predict(prompt)
        try:
            suggestions = json.loads(clean_llm_output(result))
        except:
            suggestions = [{"silver_column": c, "suggested": False} for c in silver_cols]

        # Build silver_df with suggested columns
        for s in suggestions:
            col = sanitize_column_name(s["silver_column"])
            if col not in silver_df.columns:
                silver_df[col] = pd.NA
    
    else:
        silver_cols = list(silver_df.columns)

    # --- Bronze → Silver Mapping ---
    mapping = []
    for b_col in bronze_cols:
        # Match to Silver automatically (by name or type)
        matched_col = None
        for s_col in silver_cols:
            if sanitize_column_name(b_col.lower()) == sanitize_column_name(s_col.lower()):
                matched_col = s_col
                break
        mapping.append({
            "bronze_column": b_col,
            "silver_column": matched_col or sanitize_column_name(b_col),
            "mapping_type": "schema" if matched_col else "logic"
        })

    # --- Build Silver DataFrame based on mapping ---
    silver_df_mapped = pd.DataFrame()
    for m in mapping:
        bronze_col = m['bronze_column']
        silver_col = m['silver_column']
        if bronze_col in bronze_df.columns:
            silver_df_mapped[silver_col] = bronze_df[bronze_col]
        else:
            silver_df_mapped[silver_col] = pd.NA

    # --- Save Excel mapping document ---
    file_name = f"mapping_{uuid4().hex[:8]}.xlsx"
    file_path = os.path.join(OUTPUT_DIR, file_name)
    with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
        silver_df_mapped.to_excel(writer, index=False, sheet_name="SilverData")
        bronze_df.to_excel(writer, index=False, sheet_name="BronzeData")

    # --- Generate DDL + INSERT VALUES ---
    ddl_lines = [f"CREATE TABLE {silver_name} ("]
    for col in silver_df_mapped.columns:
        dtype = silver_df_mapped[col].dtype
        if pd.api.types.is_integer_dtype(dtype):
            col_type = "INT"
        elif pd.api.types.is_float_dtype(dtype):
            col_type = "FLOAT"
        else:
            col_type = "STRING"
        ddl_lines.append(f"    {col} {col_type},")
    ddl_lines[-1] = ddl_lines[-1].rstrip(",")
    ddl_lines.append(");")

    # INSERT VALUES
    ddl_lines.append(f"\nINSERT INTO {silver_name} ({', '.join(silver_df_mapped.columns)}) VALUES")
    for i in range(len(silver_df_mapped)):
        vals = []
        for col in silver_df_mapped.columns:
            val = silver_df_mapped.iloc[i][col]
            if pd.isna(val):
                vals.append("NULL")
            elif isinstance(val, str):
                vals.append(f"'{val}'")
            else:
                vals.append(str(val))
        ddl_lines.append(f"({', '.join(vals)}),")
    ddl_lines[-1] = ddl_lines[-1].rstrip(",") + ";"

    ddl_text = "\n".join(ddl_lines)

    # Record history
    MAPPING_HISTORY.append({
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "bronze_table": bronze_name,
        "silver_table": silver_name,
        "mapping_file": file_name,
        "ddl": ddl_text
    })

    return {
        "mapping_file": file_name,
        "ddl": ddl_text
    }


# --- FastAPI endpoint for Excel download ---
def download_mapping_file(file_name: str):
    file_path = os.path.join(OUTPUT_DIR, file_name)
    if os.path.exists(file_path):
        return FileResponse(
            path=file_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=file_name
        )
    return {"error": "File not found"}

# --- FastAPI Entry Point ---
def invoke_mapping(state: dict) -> dict:
    bronze_df = parse_excel(state["bronze_file"], state["bronze_filename"])
    silver_df = parse_excel(state["silver_file"], state["silver_filename"])
    return map_bronze_to_silver(
        bronze_df, silver_df, state["bronze_name"], state["silver_name"]
    )
