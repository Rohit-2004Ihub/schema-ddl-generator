import re

def sanitize_column_name(col: str) -> str:
    """Sanitize column names for Databricks/Snowflake compatibility."""
    # Lowercase, replace spaces & invalid chars with underscores
    col = col.strip().lower()
    col = re.sub(r"[^a-zA-Z0-9_]", "_", col)
    # Ensure it doesn't start with a number
    if re.match(r"^\d", col):
        col = f"col_{col}"
    return col

def generate_databricks_ddl(columns: dict, table_name: str):
    cols = ",\n  ".join([f"{col} {dtype}" for col, dtype in columns.items()])
    return f"CREATE TABLE {table_name} (\n  {cols}\n) USING DELTA;"

def generate_snowflake_ddl(columns: dict, table_name: str):
    cols = ",\n  ".join([f"{col} {dtype}" for col, dtype in columns.items()])
    return f"CREATE TABLE {table_name} (\n  {cols}\n);"