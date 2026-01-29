import pandas as pd
import numpy as np
from pandas.api.types import is_string_dtype


def auto_parse_dates(df_list):
    """
    Automatically parse any column whose name contains 'date' or '_ts' as datetime.

    Handles pandas' new string dtype safely by:
    - casting string columns back to object before converting
    - assigning a datetime64 column in place
    """
    processed_dfs = []
    for df in df_list:
        # Create an explicit copy to avoid the SettingWithCopyWarning
        df = df.copy()

        date_cols = [col for col in df.columns if "date" in col.lower() or "_ts" in col.lower()]

        for col in date_cols:
            # If the column is using pandas' StringDtype, cast to object first
            if is_string_dtype(df[col].dtype):
                df[col] = df[col].astype("object")

            # Convert to datetime; incompatibility with previous dtype is handled
            df[col] = pd.to_datetime(df[col], format="mixed", errors="coerce")

        processed_dfs.append(df)

    return processed_dfs