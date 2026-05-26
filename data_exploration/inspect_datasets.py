# data_exploration/inspect_datasets.py
import pandas as pd
import os


def inspect_dataset(file_path: str, table_name: str):
    """
    Ek dataset ko systematically inspect karo.
    Yeh 5 cheezein hamesha dekho.
    """
    df = pd.read_csv(file_path)

    print(f"\n{'='*60}")
    print(f"TABLE: {table_name}")
    print(f"{'='*60}")

    # 1. Shape — kitni rows, kitne columns
    print(f"\n1. SHAPE: {df.shape[0]:,} rows × {df.shape[1]} columns")

    # 2. Column names aur types
    print(f"\n2. COLUMNS & TYPES:")
    for col in df.columns:
        null_pct = df[col].isnull().mean() * 100
        print(f"   {col:<35} {str(df[col].dtype):<12} nulls: {null_pct:.1f}%")

    # 3. Sample data — pehle 3 rows
    print(f"\n3. SAMPLE ROWS:")
    print(df.head(3).to_string())

    # 4. Primary key candidates — uniqueness check
    print(f"\n4. UNIQUENESS CHECK (potential PKs):")
    for col in df.columns:
        if "id" in col.lower():
            unique_pct = df[col].nunique() / len(df) * 100
            print(f"   {col:<35} unique: {df[col].nunique():,} ({unique_pct:.1f}%)")

    # 5. Numeric columns — basic stats
    numeric_cols = df.select_dtypes(include="number").columns
    if len(numeric_cols) > 0:
        print(f"\n5. NUMERIC STATS:")
        print(df[numeric_cols].describe().round(2).to_string())

    return df


# Sab datasets ek ek inspect karo
datasets = {
    "orders": "data/raw/orders.csv",
    "order_items": "data/raw/order_items.csv",
    "customers": "data/raw/customers.csv",
    "products": "data/raw/products.csv",
    "stores": "data/raw/stores.csv",
}

dfs = {}
for name, path in datasets.items():
    if os.path.exists(path):
        dfs[name] = inspect_dataset(path, name)
