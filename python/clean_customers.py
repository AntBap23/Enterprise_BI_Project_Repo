import pandas as pd
import numpy as np

def clean_customers(df):
    # 1. Create a local copy to break the link to the slice
    df = df.copy()
    
    # 2. Use .loc for explicit assignment
    # Extract digits, remove leading zeros, add 'cust-'
    extracted = df['customer_code'].astype(str).str.extract(r'(\d+)')[0].fillna('0')
    df.loc[:, 'customer_code'] = 'cust-' + extracted.astype(int).astype(str)
    
    # Clean segments
    df.loc[:, 'customer_segment'] = df['customer_segment'].str.lower().str.strip()
    
    # 3. Drop duplicates and return the new DataFrame
    df = df.drop_duplicates(subset=['customer_code'], keep='first').reset_index(drop=True)
    
    return df