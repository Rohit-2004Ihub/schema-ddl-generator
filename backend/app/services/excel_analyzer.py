import pandas as pd

def analyze_excel(file_bytes: bytes, nrows: int = 50):
    """
    Analyze the first nrows of Excel file to infer column names and types
    Returns dict: { column_name: inferred_type }
    """
    # Load Excel
    df = pd.read_excel(file_bytes, nrows=nrows)

    # Infer types
    type_map = {}
    for col in df.columns:
        if pd.api.types.is_integer_dtype(df[col]):
            dtype = "INT"
        elif pd.api.types.is_float_dtype(df[col]):
            dtype = "FLOAT"
        elif pd.api.types.is_bool_dtype(df[col]):
            dtype = "BOOLEAN"
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            dtype = "TIMESTAMP"
        else:
            max_len = df[col].astype(str).map(len).max()
            dtype = f"VARCHAR({max_len if max_len>0 else 50})"
        type_map[col] = dtype

    return type_map, df.head(nrows).to_dict('records')
