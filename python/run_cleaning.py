import os
import pandas as pd
import numpy as np
from clean_regions import clean_regions
from clean_orders import clean_orders
from clean_products import clean_products
from clean_stores import clean_stores
from clean_customers import clean_customers
from standard_clean import auto_parse_dates


def run_cleaning():
    # Ensure the cleaned output directory exists
    output_dir = "data/cleaned"
    os.makedirs(output_dir, exist_ok=True)

    # Read raw data
    regions = pd.read_csv("data/raw/raw_regions.csv")
    orders = pd.read_csv("data/raw/raw_orders.csv")
    products = pd.read_csv("data/raw/raw_products.csv")
    stores = pd.read_csv("data/raw/raw_stores.csv")
    customers = pd.read_csv("data/raw/raw_customers.csv")

    # Parse dates across all DataFrames
    regions, orders, products, stores, customers = auto_parse_dates(
        [regions, orders, products, stores, customers]
    )

    # ------------------------------------------------------------------
    # Per-table cleaning
    # ------------------------------------------------------------------
    regions_clean = clean_regions(regions)
    products_clean = clean_products(products)
    stores_clean = clean_stores(stores)
    customers_clean = clean_customers(customers)
    orders_clean = clean_orders(orders)

    # ------------------------------------------------------------------
    # Enforce referential integrity at the cleaning stage:
    # drop orders whose store_code / product_code do not exist
    # in the cleaned dimension tables, except for sentinel -0 codes.
    # ------------------------------------------------------------------
    valid_store_codes = set(stores_clean["store_code"].unique())
    valid_product_codes = set(products_clean["product_code"].unique())

    orders_clean = orders_clean[
        (
            orders_clean["store_code"].isin(valid_store_codes)
            | (orders_clean["store_code"] == "store-0")
        )
        & (
            orders_clean["product_code"].isin(valid_product_codes)
            | (orders_clean["product_code"] == "prd-0")
        )
    ]

    # ------------------------------------------------------------------
    # Write cleaned outputs
    # ------------------------------------------------------------------
    regions_clean.to_csv(os.path.join(output_dir, "regions.csv"), index=False)
    products_clean.to_csv(os.path.join(output_dir, "products.csv"), index=False)
    stores_clean.to_csv(os.path.join(output_dir, "stores.csv"), index=False)
    customers_clean.to_csv(os.path.join(output_dir, "customers.csv"), index=False)
    orders_clean.to_csv(os.path.join(output_dir, "orders.csv"), index=False)

    return True


if __name__ == "__main__":
    # Allow running this file directly: `python python/run_cleaning.py`
    run_cleaning()