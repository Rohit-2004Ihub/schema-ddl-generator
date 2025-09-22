def generate_databricks_ddl(columns: dict, table_name: str):
    cols = ",\n  ".join([f"{col} {dtype}" for col, dtype in columns.items()])
    return f"CREATE TABLE {table_name} (\n  {cols}\n) USING DELTA;"

def generate_snowflake_ddl(columns: dict, table_name: str):
    cols = ",\n  ".join([f"{col} {dtype}" for col, dtype in columns.items()])
    return f"CREATE TABLE {table_name} (\n  {cols}\n);"
