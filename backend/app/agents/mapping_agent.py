import pandas as pd
import io
import os
import json
import re
from uuid import uuid4
import numpy as np
from datetime import datetime
from difflib import get_close_matches  # Retained as fallback
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

# --- Helper: Clean NaN/NaT/pd.NA for JSON ---
def _clean_for_json(obj):
    if isinstance(obj, float) and (np.isnan(obj) or np.isinf(obj)):
        return None
    if obj is pd.NA:
        return None
    if isinstance(obj, dict):
        return {k: _clean_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean_for_json(v) for v in obj]
    return obj

# --- Smart Bronze → Silver Mapping and DDL ---
def map_bronze_to_silver(
    bronze_df: pd.DataFrame,
    silver_df: pd.DataFrame = None,
    bronze_name: str = "bronze_table",
    silver_name: str = "silver_table",
    strict_mode: bool = True
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

    # Enhanced: Use LLM for semantic mapping (replaces exact/fuzzy)
    mapping = []
    silver_df_mapped = pd.DataFrame(index=bronze_df.index)  # Initialize with same rows as bronze
    try:
        llm = get_llm()
        prompt = f"""
You are a data mapping expert. Given bronze columns: {bronze_cols} with sample data: {bronze_df.head(5).to_dict()},
and silver columns: {silver_cols},
Suggest mappings as a JSON dict where keys are bronze columns and values are the best matching silver column (or null if no match).
Consider names, types, and sample values semantically.
"""
        result = llm.predict(prompt)
        ai_mappings = json.loads(clean_llm_output(result))

        for b_col in bronze_cols:
            s_col = ai_mappings.get(b_col)
            if s_col in silver_cols:
                sample_data = _clean_for_json(bronze_df[b_col].head(5).tolist())  # Clean NaN here
                mapping.append({
                    "bronze_column": b_col,
                    "silver_column": s_col,
                    "mapping_type": "ai_matched",
                    "transformation": "direct_copy",
                    "sample_data": sample_data
                })
                silver_df_mapped[s_col] = bronze_df[b_col]
            else:
                if strict_mode:
                    sample_data = _clean_for_json(bronze_df[b_col].head(5).tolist())  # Clean NaN here
                    mapping.append({
                        "bronze_column": b_col,
                        "silver_column": None,
                        "mapping_type": "unmapped",
                        "transformation": "none",
                        "sample_data": sample_data
                    })
                else:
                    mapped_name = f"mapped_{sanitize_column_name(b_col)}"
                    sample_data = _clean_for_json(bronze_df[b_col].head(5).tolist())  # Clean NaN here
                    mapping.append({
                        "bronze_column": b_col,
                        "silver_column": mapped_name,
                        "mapping_type": "manual_needed",
                        "transformation": "direct_copy",
                        "sample_data": sample_data
                    })
                    silver_df_mapped[mapped_name] = bronze_df[b_col]
    except:
        # Fallback to original fuzzy if LLM fails
        for b_col in bronze_cols:
            exact_match = next(
                (s for s in silver_cols if sanitize_column_name(s.lower()) == sanitize_column_name(b_col.lower())),
                None
            )
            if not exact_match:
                close = get_close_matches(b_col, silver_cols, n=1, cutoff=0.6)
                exact_match = close[0] if close else None

            if exact_match:
                sample_data = _clean_for_json(bronze_df[b_col].head(5).tolist())  # Clean NaN here
                mapping.append({
                    "bronze_column": b_col,
                    "silver_column": exact_match,
                    "mapping_type": "fuzzy_matched",
                    "transformation": "direct_copy",
                    "sample_data": sample_data
                })
                silver_df_mapped[exact_match] = bronze_df[b_col]
            else:
                if strict_mode:
                    sample_data = _clean_for_json(bronze_df[b_col].head(5).tolist())  # Clean NaN here
                    mapping.append({
                        "bronze_column": b_col,
                        "silver_column": None,
                        "mapping_type": "unmapped",
                        "transformation": "none",
                        "sample_data": sample_data
                    })
                else:
                    mapped_name = f"mapped_{sanitize_column_name(b_col)}"
                    sample_data = _clean_for_json(bronze_df[b_col].head(5).tolist())  # Clean NaN here
                    mapping.append({
                        "bronze_column": b_col,
                        "silver_column": mapped_name,
                        "mapping_type": "manual_needed",
                        "transformation": "direct_copy",
                        "sample_data": sample_data
                    })
                    silver_df_mapped[mapped_name] = bronze_df[b_col]

    # --- Fill additional Silver columns dynamically using AI, deriving from existing silver data ---
    additional_cols = [c for c in silver_cols if c not in silver_df_mapped.columns]
    if additional_cols:
        llm = get_llm()
        # Use existing silver data for derivation
        silver_records = silver_df_mapped.to_dict(orient="records")

        for col in additional_cols:
            # Detect if likely geography-related    or name-related
            is_geo_col = 'country' in col.lower() or 'location' in col.lower()
            is_name_col = 'name' in col.lower()  # e.g., first_name, last_name
            
            if is_geo_col:
                prompt = f"""
You are an expert geographer with extensive knowledge of world locations, cities, and countries.

Given the following Silver table data rows:
{silver_records}
Bronze rows: {bronze_df.to_dict(orient="records")}

For each row, infer the value for the column '{col}' based on related fields like city or location columns. Use your world knowledge to make accurate inferences.
Assume common cities and their countries. Always attempt to infer if possible.
Examples:
- If a row has 'City_from': 'Mumbai', infer '{col}': 'India' (as Mumbai is a city in India).
- If a row has 'City_from': 'Sydney', infer '{col}': 'Australia' (as Sydney is a city in Australia).
- If a row has 'City_from': 'Seattle', infer '{col}': 'USA' (as Seattle is a city in USA).
- If a row has 'City_from': 'Nice', infer '{col}': 'France'.
- If a row has 'City_from': 'New York', infer '{col}': 'USA'.
- If a row has 'City_from': 'Tokyo', infer '{col}': 'Japan'.

If the value cannot be reasonably inferred (e.g., ambiguous or unknown city), use NULL.
Respond strictly as a JSON array of values (strings or null) matching the number of rows. No extra text.
"""
            elif is_name_col:
                prompt = f"""
You are a name parsing expert skilled in splitting full names into components.

Given the following Silver table data rows:
{silver_records}
Bronze rows: {bronze_df.to_dict(orient="records")}

For each row, generate the value for '{col}' by parsing related name fields (e.g., full 'Name'). Assume standard 'First Last' format and split on space (first part as first_name, last part as last_name, ignore middle if present).
Examples for first_name:
- From 'Name': 'Jordan Kumar', infer 'first_name': 'Jordan'.
- From 'Name': 'Casey Rao', infer 'first_name': 'Casey'.
- From 'Name': 'Reese Patel', infer 'first_name': 'Reese'.
Examples for last_name:
- From 'Name': 'Jordan Kumar', infer 'last_name': 'Kumar'.
- From 'Name': 'Casey Rao', infer 'last_name': 'Rao'.
- From 'Name': 'Reese Patel', infer 'last_name': 'Patel'.

If cannot parse reliably (e.g., single word name), use NULL.
Respond strictly as a JSON array of values (strings or null) matching the number of rows. No extra text.
"""
            else:
                prompt = f"""
You are a data transformation assistant with extensive world knowledge, including geography, calculations, and categorizations.

Given the following Silver table data with existing columns populated:
{silver_records}
Bronze rows: {bronze_df.to_dict(orient="records")}

Generate values for the additional column '{col}' for each row, deriving from the other existing column values where possible (e.g., infer from related fields). Use your world knowledge if necessary to make reasonable inferences.
Examples:
- If '{col}' is a country column and there's a city field, infer the country based on known city locations (e.g., 'Mumbai' -> 'India', 'Sydney' -> 'Australia', 'Seattle' -> 'USA').
- If it's an age category, classify based on numeric age (e.g., 38 -> 'Adult', 41 -> 'Adult').
- For calculations, compute from numeric fields (e.g., score percentage).

If a value cannot be reasonably derived or inferred, return NULL.
Respond strictly as a JSON array of values (strings, numbers, or null) with length equal to the number of rows. No extra text.
"""
            try:
                result = llm.predict(prompt)
                values = json.loads(clean_llm_output(result))
                if isinstance(values, list) and len(values) == len(bronze_df):
                    silver_df_mapped[col] = values
                    sample_data = _clean_for_json(pd.Series(values).head(5).tolist())  # Clean NaN here
                    # Add to mapping for derived columns
                    mapping.append({
                        "bronze_column": None,
                        "silver_column": col,
                        "mapping_type": "ai_derived",
                        "transformation": "ai_generated_from_existing",
                        "sample_data": sample_data
                    })
                else:
                    silver_df_mapped[col] = pd.NA
                    mapping.append({
                        "bronze_column": None,
                        "silver_column": col,
                        "mapping_type": "unmapped",
                        "transformation": "none",
                        "sample_data": []
                    })
            except:
                silver_df_mapped[col] = pd.NA
                mapping.append({
                    "bronze_column": None,
                    "silver_column": col,
                    "mapping_type": "unmapped",
                    "transformation": "none",
                    "sample_data": []
                })

    # --- Infer expected data types using LLM ---
    llm = get_llm()
    prompt = f"""
You are a data type inference expert.
Given silver columns: {silver_cols} and sample data: {silver_df_mapped.head(5).to_dict()},
Suggest expected data types for each column as a JSON dict where keys are columns and values are 'int', 'float', 'datetime', or 'str'.
Ignore actual data types in samples and any invalid values; base solely on column names and typical usage.
Examples:
- 'age_of_the_person': 'int' (always, even if samples have strings like 'Thirty seven')
- 'Score_get': 'float'
- 'SignupDate_in_work': 'datetime'
- 'ID': 'int'
- 'Name': 'str'
"""
    try:
        result = llm.predict(prompt)
        expected_types = json.loads(clean_llm_output(result))
    except:
        expected_types = {}  # Fallback to no validation if fails

    # --- Data Validation: Remove invalid rows based on expected types ---
    removed_rows = []
    valid_mask = pd.Series([True] * len(silver_df_mapped), index=silver_df_mapped.index)
    for idx, row in silver_df_mapped.iterrows():
        invalid = False
        for col in silver_df_mapped.columns:
            exp_type = expected_types.get(col, 'str')  # Default to str
            val = row[col]
            if pd.isna(val):
                continue  # Allow NULLs
            try:
                if exp_type == 'int':
                    int(val)
                elif exp_type == 'float':
                    float(val)
                elif exp_type == 'datetime':
                    pd.to_datetime(val)
                # 'str' always passes
            except ValueError:
                invalid = True
                removed_rows.append({
                    "row_index": idx,
                    "reason": f"Invalid type in {col}: expected {exp_type}, got {type(val).__name__}",
                    **row.to_dict()
                })
                break  # Remove entire row on first invalid
        if invalid:
            valid_mask[idx] = False

    # Apply mask to keep only valid rows
    silver_df_mapped = silver_df_mapped[valid_mask]

    # Save Excel mapping file (enhanced with transformation column and RemovedRows)
    file_name = f"mapping_{uuid4().hex[:8]}.xlsx"
    file_path = os.path.join(OUTPUT_DIR, file_name)
    with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
        pd.DataFrame(mapping).to_excel(writer, index=False, sheet_name="Mapping")
        bronze_df.to_excel(writer, index=False, sheet_name="BronzeData")
        silver_df_mapped.to_excel(writer, index=False, sheet_name="SilverData")
        if removed_rows:
            pd.DataFrame(removed_rows).to_excel(writer, index=False, sheet_name="RemovedRows")

    # Generate DDL (unchanged)
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

    # Generate INSERT statements (unchanged, but now on validated data)
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

    # Record mapping history (unchanged)
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