
import sys
import pandas as pd
from pandas.api.types import (
    is_string_dtype,
    is_integer_dtype,
    is_float_dtype,
    is_datetime64_any_dtype,
)

# -------------------------
# CONFIG
# -------------------------
CLEAN_DIR = "data/cleaned"

TABLE_PATHS = {
    "regions": f"{CLEAN_DIR}/regions.csv",
    "products": f"{CLEAN_DIR}/products.csv",
    "stores": f"{CLEAN_DIR}/stores.csv",
    "customers": f"{CLEAN_DIR}/customers.csv",
    "orders": f"{CLEAN_DIR}/orders.csv",
}

SCHEMAS = {
    "regions": {
        "required_columns": {"region_code", "region_name", "active_flag"},
        "allow_nulls": set(),
        "strings": {"region_code", "region_name", "active_flag"},
        "ints": set(),
        "floats": set(),
        "datetimes": set(),
        "unique_keys": [("region_code",), ("region_name",)],
        "allowed_values": {"active_flag": {"Y", "N"}},
    },
    "products": {
        "required_columns": {"product_code", "product_name", "category", "base_price", "active_flag"},
        "allow_nulls": set(),
        "strings": {"product_code", "product_name", "category", "active_flag"},
        "ints": set(),
        "floats": {"base_price"},
        "datetimes": set(),
        "unique_keys": [("product_code",)],
        "allowed_values": {"active_flag": {"Y", "N"}},
        "min_values": {"base_price": 0.0},
    },
    "stores": {
        "required_columns": {
            "store_code", "store_name", "city", "state", "region_name",
            "open_date", "close_date", "square_feet"
        },
        "allow_nulls": {"close_date"},  # closed stores may be blank; open stores often have NaN here
        "strings": {"store_code", "store_name", "city", "state", "region_name"},
        "ints": set(),
        "floats": {"square_feet"},
        "datetimes": {"open_date", "close_date"},
        "unique_keys": [("store_code",)],
        "min_values": {"square_feet": 1.0},
    },
    "customers": {
        "required_columns": {"customer_code", "signup_date", "customer_segment", "email", "loyalty_id"},
        # Allow email to be null / non-unique, as requested
        "allow_nulls": {"loyalty_id", "email"},  # loyalty_id & email can be missing
        "strings": {"customer_code", "customer_segment", "email", "loyalty_id"},
        "ints": set(),
        "floats": set(),
        "datetimes": {"signup_date"},
        # Only enforce uniqueness on customer_code, not email
        "unique_keys": [("customer_code",)],
    },
    "orders": {
        "required_columns": {
            "order_code", "order_ts", "delivery_ts", "store_code", "customer_code",
            "order_type", "order_status", "line_number", "product_code",
            "quantity", "unit_price", "unit_cost"
        },
        "allow_nulls": {"delivery_ts"},  # common for cancelled/undelivered orders
        "strings": {"order_code", "store_code", "customer_code", "order_type", "order_status", "product_code"},
        "ints": {"line_number", "quantity"},
        "floats": {"unit_price", "unit_cost"},
        "datetimes": {"order_ts", "delivery_ts"},
        "unique_keys": [("order_code", "line_number")],
        "min_values": {"line_number": 1, "quantity": 0, "unit_price": 0.0, "unit_cost": 0.0},
    },
}


# -------------------------
# LOADING (CSV -> typed DF)
# -------------------------
def load_table(name: str) -> pd.DataFrame:
    schema = SCHEMAS[name]
    path = TABLE_PATHS[name]

    # Read everything as-is first (CSV has no types)
    df = pd.read_csv(path)

    # Parse datetimes (coerce invalid -> NaT)
    for col in schema.get("datetimes", set()):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Coerce ints (use nullable Int64 so NaNs are supported during validation)
    for col in schema.get("ints", set()):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Coerce floats
    for col in schema.get("floats", set()):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Force strings (nullable string dtype)
    for col in schema.get("strings", set()):
        if col in df.columns:
            df[col] = df[col].astype("string")

    return df


# -------------------------
# VALIDATION HELPERS
# -------------------------
def _fail(msg: str) -> None:
    print("❌", msg)

def _warn(msg: str) -> None:
    print("⚠️", msg)

def _ok(msg: str) -> None:
    print("✅", msg)

def validate_schema(name: str, df: pd.DataFrame) -> bool:
    schema = SCHEMAS[name]
    ok = True

    required = schema["required_columns"]
    cols = set(df.columns)

    missing = required - cols
    extra = cols - required

    if missing:
        _fail(f"[{name}] Missing columns: {sorted(missing)}")
        ok = False
    if extra:
        _warn(f"[{name}] Extra columns (unexpected): {sorted(extra)}")

    return ok

def validate_nulls(name: str, df: pd.DataFrame) -> bool:
    schema = SCHEMAS[name]
    ok = True
    allow_nulls = schema.get("allow_nulls", set())

    for col in schema["required_columns"]:
        if col not in df.columns:
            continue
        if col not in allow_nulls and df[col].isna().any():
            n = int(df[col].isna().sum())
            _fail(f"[{name}] Nulls not allowed in '{col}' (found {n})")
            ok = False

    return ok

def validate_dtypes(name: str, df: pd.DataFrame) -> bool:
    schema = SCHEMAS[name]
    ok = True

    # strings
    for col in schema.get("strings", set()):
        if col in df.columns and not is_string_dtype(df[col]):
            _fail(f"[{name}] '{col}' dtype is {df[col].dtype}, expected string")
            ok = False

    # ints
    for col in schema.get("ints", set()):
        if col in df.columns and not is_integer_dtype(df[col]):
            _fail(f"[{name}] '{col}' dtype is {df[col].dtype}, expected integer")
            ok = False

    # floats
    for col in schema.get("floats", set()):
        if col in df.columns and not (is_float_dtype(df[col]) or is_integer_dtype(df[col])):
            _fail(f"[{name}] '{col}' dtype is {df[col].dtype}, expected float")
            ok = False

    # datetimes
    for col in schema.get("datetimes", set()):
        if col in df.columns and not is_datetime64_any_dtype(df[col]):
            _fail(f"[{name}] '{col}' dtype is {df[col].dtype}, expected datetime")
            ok = False

    # This catches the “CSV parse failed” case: to_numeric(errors='coerce') creates NaNs
    # If a numeric column has NaNs and it's not allowed, null validation will catch it too.

    return ok

def validate_allowed_values(name: str, df: pd.DataFrame) -> bool:
    schema = SCHEMAS[name]
    ok = True
    allowed_values = schema.get("allowed_values", {})

    for col, allowed in allowed_values.items():
        if col not in df.columns:
            continue
        bad = df.loc[~df[col].isna() & ~df[col].isin(list(allowed)), col].unique()
        if len(bad) > 0:
            _fail(f"[{name}] '{col}' has invalid values: {bad[:10]}")
            ok = False

    return ok

def validate_min_values(name: str, df: pd.DataFrame) -> bool:
    schema = SCHEMAS[name]
    ok = True
    mins = schema.get("min_values", {})

    for col, min_val in mins.items():
        if col not in df.columns:
            continue
        # ignore nulls; null rules handled elsewhere
        bad = df.loc[~df[col].isna() & (df[col] < min_val), col]
        if not bad.empty:
            _fail(f"[{name}] '{col}' has values < {min_val} (examples: {bad.head(5).tolist()})")
            ok = False

    return ok

def validate_uniqueness(name: str, df: pd.DataFrame) -> bool:
    schema = SCHEMAS[name]
    ok = True

    for key_cols in schema.get("unique_keys", []):
        cols = list(key_cols)
        if any(c not in df.columns for c in cols):
            continue
        dupes = df[df.duplicated(subset=cols, keep=False)]
        if not dupes.empty:
            _fail(f"[{name}] Duplicate rows found for unique key {cols} (showing 5):")
            print(dupes[cols].head(5).to_string(index=False))
            ok = False

    return ok

def validate_referential_integrity(tables: dict[str, pd.DataFrame]) -> bool:
    ok = True

    regions = tables["regions"]
    products = tables["products"]
    stores = tables["stores"]
    customers = tables["customers"]
    orders = tables["orders"]

    # orders.store_code -> stores.store_code
    # Ignore sentinel 'store-0' which represents a null store
    missing_store = orders.loc[
        (~orders["store_code"].isin(stores["store_code"])) & (orders["store_code"] != "store-0"),
        "store_code",
    ].dropna().unique()
    if len(missing_store) > 0:
        _fail(f"[FK] orders.store_code has values not in stores.store_code (examples: {missing_store[:10]})")
        ok = False

    # orders.customer_code -> customers.customer_code
    # Ignore sentinel 'cust-0' which represents a null customer
    missing_cust = orders.loc[
        (~orders["customer_code"].isin(customers["customer_code"])) & (orders["customer_code"] != "cust-0"),
        "customer_code",
    ].dropna().unique()
    if len(missing_cust) > 0:
        _fail(f"[FK] orders.customer_code has values not in customers.customer_code (examples: {missing_cust[:10]})")
        ok = False

    # orders.product_code -> products.product_code
    # Ignore sentinel 'prd-0' which represents a null product
    missing_prod = orders.loc[
        (~orders["product_code"].isin(products["product_code"])) & (orders["product_code"] != "prd-0"),
        "product_code",
    ].dropna().unique()
    if len(missing_prod) > 0:
        _fail(f"[FK] orders.product_code has values not in products.product_code (examples: {missing_prod[:10]})")
        ok = False

    # stores.region_name -> regions.region_name
    missing_region = stores.loc[~stores["region_name"].isin(regions["region_name"]), "region_name"].dropna().unique()
    if len(missing_region) > 0:
        _fail(f"[FK] stores.region_name has values not in regions.region_name (examples: {missing_region[:10]})")
        ok = False

    return ok


# -------------------------
# RUN ALL
# -------------------------
def run_validation() -> None:
    tables: dict[str, pd.DataFrame] = {}

    # Load
    for name in TABLE_PATHS.keys():
        tables[name] = load_table(name)
        _ok(f"Loaded {name}: {tables[name].shape[0]} rows, {tables[name].shape[1]} cols")

    all_ok = True

    # Per-table checks
    for name, df in tables.items():
        all_ok &= validate_schema(name, df)
        all_ok &= validate_nulls(name, df)
        all_ok &= validate_dtypes(name, df)
        all_ok &= validate_allowed_values(name, df)
        all_ok &= validate_min_values(name, df)
        all_ok &= validate_uniqueness(name, df)

    # Cross-table checks
    all_ok &= validate_referential_integrity(tables)

    if not all_ok:
        raise SystemExit("\nValidation failed. Fix issues before loading to DB.\n")

    print("\n✅ All validation checks passed.\n")


if __name__ == "__main__":
    run_validation()
