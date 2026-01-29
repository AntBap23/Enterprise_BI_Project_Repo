import pandas as pd
import numpy as np


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean orders and enforce:
    - Standardised status/type text
    - Normalised IDs (ord-/cust-/store-/prd-)
    - Non-negative numeric quantities/prices/costs
    - Drop rows with missing unit_cost
    - Ensure (order_code, line_number) is unique by reassigning new order_codes
      starting after the current max order number.
    Special sentinel IDs 'cust-0', 'prd-0', 'store-0' are preserved to represent nulls.
    """
    df = df.copy()

    # order status and type whitespace stripped and lowered
    df["order_status"] = df["order_status"].astype(str).str.lower().str.strip()
    df["order_type"] = df["order_type"].astype(str).str.lower().str.strip()

    # Helper to handle extraction and conversion safely
    def standardize_id(series, prefix):
        # 1. Extract digits
        # 2. fillna('0') -> sentinel 0 for nulls
        # 3. Convert to int to drop leading zeros, then back to str
        extracted = series.astype(str).str.extract(r"(\d+)")[0].fillna("0")
        return prefix + "-" + extracted.astype(int).astype(str)

    # Apply to your ID columns
    df["order_code"] = standardize_id(df["order_code"], "ord")
    df["customer_code"] = standardize_id(df["customer_code"], "cust")
    df["store_code"] = standardize_id(df["store_code"], "store")
    df["product_code"] = standardize_id(df["product_code"], "prd")

    # Numeric fields: coerce to numeric and enforce >= 0
    for col in ["line_number", "quantity", "unit_price", "unit_cost"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop clearly invalid rows
    df = df[df["unit_cost"].notna()]  # must have cost
    df = df[df["quantity"] >= 0]      # no negative quantities
    df = df[df["unit_price"] >= 0]    # no negative prices
    df = df[df["unit_cost"] >= 0]     # no negative costs

    # ------------------------------------------------------------------
    # Fix duplicate primary keys (order_code, line_number)
    # ------------------------------------------------------------------
    # Find current max order number
    order_nums = (
        df["order_code"].astype(str).str.extract(r"(\d+)")[0].fillna("0").astype(int)
    )
    max_order_num = int(order_nums.max()) if not order_nums.empty else 0

    # Identify duplicates (beyond the first occurrence)
    dup_mask = df.duplicated(subset=["order_code", "line_number"], keep="first")
    dup_indices = df.index[dup_mask]

    # Reassign new order_codes for duplicates, starting after max_order_num
    for offset, idx in enumerate(dup_indices, start=1):
        new_num = max_order_num + offset
        df.at[idx, "order_code"] = f"ord-{new_num}"

    return df