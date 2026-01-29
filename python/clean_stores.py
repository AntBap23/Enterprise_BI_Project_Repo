import pandas as pd
import numpy as np


def clean_stores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean store attributes and enforce:
    - Standardised codes / text
    - Non-null, positive square_feet
    - Unique store_code
    Region names are normalised to match the regions dimension.
    """
    df = df.copy()

    df["state"] = df["state"].astype(str).str.strip().str.upper()

    # Normalise region_name similarly to regions so FK passes
    df["region_name"] = (
        df["region_name"]
        .astype(str)
        .str.lower()
        .str.replace("region", "", regex=False)
        .str.strip()
        .str.title()
    )

    # Standardise store_code -> 'store-<int>'
    extracted = df["store_code"].astype(str).str.extract(r"(\d+)")[0].fillna("0")
    df["store_code"] = "store-" + extracted.astype(int).astype(str)

    df["city"] = df["city"].astype(str).str.strip().str.title()
    df["store_name"] = df["store_name"].astype(str).str.strip().str.title()

    # square_feet: numeric, non-null, >= 1
    df["square_feet"] = pd.to_numeric(df["square_feet"], errors="coerce")
    df = df[df["square_feet"].notna()]          # drop rows with missing size
    df = df[df["square_feet"] >= 1]             # enforce positive area

    # De-duplicate by store_code
    df = df.drop_duplicates(subset=["store_code"], keep="first").reset_index(drop=True)

    return df