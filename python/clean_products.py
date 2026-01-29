import pandas as pd
import numpy as np


def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardise product fields and enforce data quality:
    - Clean product_name / category strings
    - Normalise product_code to 'prd-<int>'
    - Ensure base_price is non-null, numeric, and >= 0
    - Drop duplicate product_code, keeping the first valid row
    """
    df = df.copy()

    # String clean-up
    df["product_name"] = df["product_name"].astype(str).str.strip().str.title()
    df["category"] = df["category"].astype(str).str.strip().str.title()

    # Standardise product_code -> 'prd-<int>'
    extracted = df["product_code"].astype(str).str.extract(r"(\d+)")[0].fillna("0")
    df["product_code"] = "prd-" + extracted.astype(int).astype(str)

    # base_price: numeric, non-null, >= 0
    df["base_price"] = pd.to_numeric(df["base_price"], errors="coerce")
    df = df[df["base_price"].notna()]           # drop rows with missing base_price
    df = df[df["base_price"] >= 0]              # enforce non-negative

    # Drop duplicate products on product_code, keep the first occurrence
    df = df.drop_duplicates(subset=["product_code"], keep="first").reset_index(drop=True)

    return df