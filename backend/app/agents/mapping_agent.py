# app/services/mapping_service.py
import pandas as pd
import io
import os
import json
import re
from uuid import uuid4
from datetime import datetime
from difflib import get_close_matches
from fastapi.responses import FileResponse
from app.agents.schema_agent import clean_llm_output, get_llm

MAPPING_HISTORY = []
OUTPUT_DIR = "outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)


# --- Excel/CSV Parsing ---
def parse_excel(file_bytes: bytes, filename: str) -> pd.DataFrame:
    try:
        # --- Try CSV first ---
        try:
            return pd.read_csv(io.BytesIO(file_bytes), encoding="utf-8")
        except Exception:
            pass

        # --- Try Excel (xlsx/xls) ---
        try:
            return pd.read_excel(io.BytesIO(file_bytes), engine="openpyxl")
        except Exception:
            pass

        try:
            return pd.read_excel(io.BytesIO(file_bytes), engine="xlrd")
        except Exception:
            pass

        # If all fail, raise error
        raise ValueError(f"Unsupported or corrupt file format for: {filename}")

    except Exception as e:
        raise ValueError(f"File parsing failed: {str(e)}")


# --- Column Sanitization ---
def sanitize_column_name(col_name: str) -> str:
    sanitized = re.sub(r"[ ,;{}()\n\t=]", "_", col_name)
    sanitized = re.sub(r"__+", "_", sanitized)
    return sanitized.strip("_")


# --- Smart Bronze → Silver Mapping and DDL ---
def map_bronze_to_silver(
    bronze_df: pd.DataFrame,
    silver_df: pd.DataFrame = None,
    bronze_name: str = "bronze_table",
    silver_name: str = "silver_table",
    strict_mode: bool = True   # <-- NEW flag
) -> dict:

    bronze_cols = list(bronze_df.columns)

    # Auto-generate Silver columns if not provided
    if silver_df is None:
        silver_cols = [sanitize_column_name(c) for c in bronze_cols]
        silver_df = pd.DataFrame(columns=silver_cols)

        # Optional: AI suggestions for new columns
        try:
            llm = get_llm()
            prompt = f"""
You are a database assistant.
Given Bronze columns: {bronze_cols} and sample data: {bronze_df.head(5).to_dict()},
Suggest any additional useful Silver columns. Return JSON array with 'silver_column' names.
"""
            result = llm.predict(prompt)
            suggestions = json.loads(clean_llm_output(result))
            for s in suggestions:
                col = sanitize_column_name(s)
                if col not in silver_df.columns:
                    silver_df[col] = pd.NA
        except:
            pass
    else:
        silver_cols = list(silver_df.columns)

    # Map Bronze → Silver columns (exact & fuzzy)
    mapping = []
    silver_df_mapped = pd.DataFrame()

    for b_col in bronze_cols:
        exact_match = next(
            (s for s in silver_cols if sanitize_column_name(s.lower()) == sanitize_column_name(b_col.lower())),
            None
        )
        if not exact_match:
            close = get_close_matches(b_col, silver_cols, n=1, cutoff=0.6)
            exact_match = close[0] if close else None

        if exact_match:
            mapping.append({
                "bronze_column": b_col,
                "silver_column": exact_match,
                "mapping_type": "auto_matched" if sanitize_column_name(b_col.lower()) == sanitize_column_name(exact_match.lower()) else "fuzzy_matched",
                "sample_data": bronze_df[b_col].head(5).tolist()
            })
            silver_df_mapped[exact_match] = bronze_df[b_col]
        else:
            if strict_mode:
                # skip adding mapped_* columns
                mapping.append({
                    "bronze_column": b_col,
                    "silver_column": None,
                    "mapping_type": "unmapped",
                    "sample_data": bronze_df[b_col].head(5).tolist()
                })
            else:
                # allow mapped_* fallback
                mapped_name = f"mapped_{sanitize_column_name(b_col)}"
                mapping.append({
                    "bronze_column": b_col,
                    "silver_column": mapped_name,
                    "mapping_type": "manual_needed",
                    "sample_data": bronze_df[b_col].head(5).tolist()
                })
                silver_df_mapped[mapped_name] = bronze_df[b_col]

    # --- Fill additional Silver columns dynamically using AI ---
    additional_cols = [c for c in silver_cols if c not in silver_df_mapped.columns]
    if additional_cols:
        llm = get_llm()
        bronze_data_records = bronze_df.to_dict(orient="records")

        for col in additional_cols:
            prompt = f"""
You are a data transformation assistant.
Given the following Bronze table data:
{bronze_data_records}
Please generate values for the Silver column '{col}' for each row.
If a value cannot be inferred, return NULL.
Respond in JSON array of values with length equal to the number of rows in the Bronze table.
"""
            try:
                result = llm.predict(prompt)
                values = json.loads(clean_llm_output(result))
                if isinstance(values, list) and len(values) == len(bronze_df):
                    silver_df_mapped[col] = values
                else:
                    silver_df_mapped[col] = pd.NA
            except:
                silver_df_mapped[col] = pd.NA

    # Save Excel mapping file
    file_name = f"mapping_{uuid4().hex[:8]}.xlsx"
    file_path = os.path.join(OUTPUT_DIR, file_name)
    with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
        pd.DataFrame(mapping).to_excel(writer, index=False, sheet_name="Mapping")
        bronze_df.to_excel(writer, index=False, sheet_name="BronzeData")
        silver_df_mapped.to_excel(writer, index=False, sheet_name="SilverData")

    # Generate DDL
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

    # Generate INSERT statements
    ddl_lines.append(f"\nINSERT INTO {silver_name} ({', '.join(silver_df_mapped.columns)}) VALUES")
    for i in range(len(silver_df_mapped)):
        vals = []
        for col in silver_df_mapped.columns:
            val = silver_df_mapped.iloc[i][col]
            if pd.isna(val):
                vals.append("NULL")
            elif isinstance(val, str):
                escaped_val = val.replace("'", "''")
                vals.append(f"'{escaped_val}'")
            else:
                vals.append(str(val))
        ddl_lines.append(f"({', '.join(vals)}),")
    ddl_lines[-1] = ddl_lines[-1].rstrip(",") + ";"

    ddl_text = "\n".join(ddl_lines)

    # Record mapping history
    MAPPING_HISTORY.append({
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "bronze_table": bronze_name,
        "silver_table": silver_name,
        "mapping_file": file_name,
        "ddl": ddl_text,
        "column_mapping": mapping
    })

    return {
        "mapping_file": file_name,
        "ddl": ddl_text,
        "column_mapping": mapping
    }


# --- FastAPI file download ---
def download_mapping_file(file_name: str):
    file_path = os.path.join(OUTPUT_DIR, file_name)
    if os.path.exists(file_path):
        return FileResponse(
            path=file_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=file_name
        )
    return {"error": "File not found"}


# --- FastAPI entry point ---
def invoke_mapping(state: dict) -> dict:
    bronze_df = parse_excel(state["bronze_file"], state["bronze_filename"])
    silver_df = parse_excel(state["silver_file"], state["silver_filename"]) if state.get("silver_file") else None
    return map_bronze_to_silver(
        bronze_df, silver_df, state["bronze_name"], state["silver_name"], strict_mode=True
    )
