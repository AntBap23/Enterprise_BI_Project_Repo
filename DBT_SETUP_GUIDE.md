# dbt Setup Guide

## Current Project Structure Review

### ✅ What's Already Set Up

1. **dbt_project.yml** - Main project configuration
   - Project name: `enterprise_bi_project`
   - Profile: `enterprise_bi_project`
   - Model paths configured
   - Schema configuration:
     - `staging` → views
     - `marts` → tables

2. **Model Structure** (SQL files exist, ready for you to write):
   - **Staging models** (5 files):
     - `stg_customers.sql`
     - `stg_orders.sql`
     - `stg_products.sql`
     - `stg_stores.sql`
     - `stg_regions.sql`
   
   - **Mart models** (6 files):
     - Dimensions: `dim_customer.sql`, `dim_date.sql`, `dim_product.sql`, `dim_region.sql`, `dim_store.sql`
     - Facts: `fact_orders.sql`

### ⚠️ What Needs Setup

## 1. sources.yml - Define Your Raw Tables

**Location:** `models/sources/sources.yml`

**Purpose:** Tell dbt about your raw PostgreSQL tables in the `raw` schema.

**Structure to create:**
```yaml
version: 2

sources:
  - name: raw
    description: "Raw data tables loaded from cleaned CSVs"
    schema: raw
    database: EnterpriseBI  # Your database name from .env
    
    tables:
      - name: customers
        description: "Raw customer data"
        columns:
          - name: customer_code
            description: "Standardized customer code (cust-XXX)"
          - name: customer_segment
            description: "Customer segment (lowercased, trimmed)"
          # Add other columns as you discover them
          
      - name: orders
        description: "Raw order transactions"
        columns:
          - name: order_code
            description: "Standardized order code (ord-XXX)"
          - name: customer_code
            description: "Foreign key to customers"
          - name: store_code
            description: "Foreign key to stores"
          - name: product_code
            description: "Foreign key to products"
          - name: order_status
            description: "Order status (lowercased)"
          - name: order_type
            description: "Order type (lowercased)"
          # Add date columns, amounts, etc.
          
      - name: products
        description: "Raw product catalog"
        columns:
          - name: product_code
            description: "Standardized product code (prd-XXX)"
          # Add other product attributes
          
      - name: stores
        description: "Raw store locations"
        columns:
          - name: store_code
            description: "Standardized store code (store-XXX)"
          # Add location, region info, etc.
          
      - name: regions
        description: "Raw region/geography data"
        columns:
          # Add region attributes
```

**Key Points:**
- `schema: raw` matches your `RAW_SCHEMA` from `.env`
- `database: EnterpriseBI` matches your `DB_NAME`
- You can add columns incrementally as you discover them
- Descriptions help with documentation

## 2. staging.yml - Document Staging Models

**Location:** `models/staging/staging.yml`

**Purpose:** Document your staging models and add tests.

**Structure to create:**
```yaml
version: 2

models:
  - name: stg_customers
    description: "Cleaned and standardized customer data from raw.customers"
    columns:
      - name: customer_code
        description: "Primary key - standardized customer identifier"
        tests:
          - unique
          - not_null
      - name: customer_segment
        description: "Customer segment category"
        tests:
          - accepted_values:
              values: ['consumer', 'corporate', 'home office']
              # Adjust based on your actual segments
              
  - name: stg_orders
    description: "Cleaned order transactions with standardized codes"
    columns:
      - name: order_code
        description: "Primary key - standardized order identifier"
        tests:
          - unique
          - not_null
      - name: customer_code
        description: "Foreign key to customers"
        tests:
          - not_null
          - relationships:
              to: ref('stg_customers')
              field: customer_code
      - name: store_code
        description: "Foreign key to stores"
        tests:
          - relationships:
              to: ref('stg_stores')
              field: store_code
      - name: product_code
        description: "Foreign key to products"
        tests:
          - relationships:
              to: ref('stg_products')
              field: product_code
      - name: order_status
        tests:
          - accepted_values:
              values: ['pending', 'processing', 'shipped', 'delivered', 'cancelled']
              # Adjust based on your actual statuses
              
  - name: stg_products
    description: "Cleaned product catalog"
    columns:
      - name: product_code
        description: "Primary key - standardized product identifier"
        tests:
          - unique
          - not_null
          
  - name: stg_stores
    description: "Cleaned store locations"
    columns:
      - name: store_code
        description: "Primary key - standardized store identifier"
        tests:
          - unique
          - not_null
          
  - name: stg_regions
    description: "Cleaned region/geography data"
    columns:
      - name: region_code  # Adjust based on your actual PK
        description: "Primary key"
        tests:
          - unique
          - not_null
```

**Key Points:**
- Tests validate data quality
- `relationships` tests ensure referential integrity
- `accepted_values` tests validate categorical data
- Add tests incrementally - start with primary keys

## 3. marts.yml - Document Mart Models

**Location:** `models/marts/marts.yml`

**Purpose:** Document your dimensional model (facts and dimensions).

**Structure to create:**
```yaml
version: 2

models:
  # Dimensions
  - name: dim_customer
    description: "Customer dimension table with all customer attributes"
    columns:
      - name: customer_key
        description: "Surrogate key (primary key)"
        tests:
          - unique
          - not_null
      - name: customer_code
        description: "Business key from source system"
        tests:
          - unique
          - not_null
      # Add other customer attributes
      
  - name: dim_product
    description: "Product dimension table"
    columns:
      - name: product_key
        description: "Surrogate key (primary key)"
        tests:
          - unique
          - not_null
      - name: product_code
        description: "Business key"
        tests:
          - unique
          - not_null
          
  - name: dim_store
    description: "Store dimension table"
    columns:
      - name: store_key
        description: "Surrogate key (primary key)"
        tests:
          - unique
          - not_null
      - name: store_code
        description: "Business key"
        tests:
          - unique
          - not_null
          
  - name: dim_region
    description: "Region/geography dimension table"
    columns:
      - name: region_key
        description: "Surrogate key (primary key)"
        tests:
          - unique
          - not_null
          
  - name: dim_date
    description: "Date dimension table for time-based analysis"
    columns:
      - name: date_key
        description: "Primary key (YYYYMMDD format or date)"
        tests:
          - unique
          - not_null
      - name: date
        description: "Actual date"
        tests:
          - not_null
      # Add: year, quarter, month, week, day_of_week, is_weekend, etc.
      
  # Facts
  - name: fact_orders
    description: "Order fact table with metrics and foreign keys to dimensions"
    columns:
      - name: order_key
        description: "Surrogate key (primary key)"
        tests:
          - unique
          - not_null
      - name: order_code
        description: "Business key"
        tests:
          - unique
          - not_null
      - name: customer_key
        description: "Foreign key to dim_customer"
        tests:
          - not_null
          - relationships:
              to: ref('dim_customer')
              field: customer_key
      - name: product_key
        description: "Foreign key to dim_product"
        tests:
          - not_null
          - relationships:
              to: ref('dim_product')
              field: product_key
      - name: store_key
        description: "Foreign key to dim_store"
        tests:
          - not_null
          - relationships:
              to: ref('dim_store')
              field: store_key
      - name: region_key
        description: "Foreign key to dim_region"
        tests:
          - relationships:
              to: ref('dim_region')
              field: region_key
      - name: order_date_key
        description: "Foreign key to dim_date for order date"
        tests:
          - relationships:
              to: ref('dim_date')
              field: date_key
      # Add metrics: order_amount, quantity, discount_amount, etc.
      - name: order_amount
        description: "Total order value"
        tests:
          - not_null
```

**Key Points:**
- Dimensions have surrogate keys (usually `{entity}_key`)
- Facts have foreign keys to dimensions
- Facts contain metrics/measures
- All relationships should be tested

## 4. dbt_project.yml - Review & Enhance

**Current config is good, but you might want to add:**

```yaml
# Add to your existing dbt_project.yml

models:
  enterprise_bi_project:
    staging:
      +schema: staging
      +materialized: view
      +tags: ["staging"]
    marts:
      +schema: mart
      +materialized: table
      +tags: ["marts"]
      dimensions:
        +tags: ["dimensions"]
      facts:
        +tags: ["facts"]
        
# Optional: Add seeds if you have reference data
seeds:
  enterprise_bi_project:
    +schema: seeds
    
# Optional: Add snapshots for SCD Type 2 if needed
snapshots:
  enterprise_bi_project:
    +schema: snapshots
```

## 5. Create profiles.yml (Required for dbt)

**Location:** `~/.dbt/profiles.yml` (in your home directory)

**This file connects dbt to your PostgreSQL database:**

```yaml
enterprise_bi_project:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: bapbap23  # From your .env
      password: ""  # Your password from .env (leave empty if no password)
      port: 5432
      dbname: EnterpriseBI  # From your .env DB_NAME
      schema: staging  # Default schema for development
      threads: 4  # Number of parallel queries
      
    prod:
      type: postgres
      host: localhost
      user: bapbap23
      password: ""  # Set your production password
      port: 5432
      dbname: EnterpriseBI
      schema: mart  # Production schema
      threads: 8
```

**Important:** 
- This file should NOT be committed to git (add to .gitignore)
- Create the directory: `mkdir -p ~/.dbt`
- Use environment variables or dbt's built-in secret management for passwords in production

## Setup Checklist

- [ ] Create `~/.dbt/profiles.yml` with your PostgreSQL connection
- [ ] Populate `models/sources/sources.yml` with your raw tables
- [ ] Populate `models/staging/staging.yml` with staging model docs
- [ ] Populate `models/marts/marts.yml` with mart model docs
- [ ] Test connection: `dbt debug`
- [ ] Test source freshness: `dbt source freshness`
- [ ] Write your SQL models
- [ ] Run models: `dbt run`
- [ ] Run tests: `dbt test`

## Next Steps

1. **After data is loaded:** Run `dbt debug` to verify connection
2. **Inspect raw tables:** Use `dbt show` or connect directly to see column names
3. **Write staging models:** Start with `SELECT * FROM {{ source('raw', 'customers') }}`
4. **Add tests incrementally:** Don't try to test everything at once
5. **Build incrementally:** Start with one staging model, test it, then move to marts

## Common dbt Commands

```bash
# Test connection
dbt debug

# List available models/sources
dbt list

# Run all models
dbt run

# Run specific model
dbt run --select stg_customers

# Run models with tag
dbt run --select tag:staging

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve

# Check source freshness
dbt source freshness

# Show compiled SQL
dbt compile
```
