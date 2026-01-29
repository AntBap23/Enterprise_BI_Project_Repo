import pandas as pd
import numpy as np


def clean_regions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise region names to align with store region_name values.
    Example: 'north region' -> 'North'
    """
    df = df.copy()

    df["region_name"] = (
        df["region_name"]
        .astype(str)
        .str.lower()
        .str.replace("region", "", regex=False)
        .str.strip()
        .str.title()
    )

    return df