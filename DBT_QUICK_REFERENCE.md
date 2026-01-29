# dbt Quick Reference

## Current State Review

### ✅ Already Configured
- `dbt_project.yml` - Project structure defined
- Model file structure (11 SQL files ready for you to write)
- Schema configuration (staging=views, marts=tables)

### 📝 YAML Files to Populate (No SQL - Just Documentation)

1. **`models/sources/sources.yml`** - Define your 5 raw tables
   - customers, orders, products, stores, regions
   - All in `raw` schema
   - Document columns as you discover them

2. **`models/staging/staging.yml`** - Document your 5 staging models
   - Add tests for primary keys (unique, not_null)
   - Add relationship tests for foreign keys
   - Add accepted_values tests for categorical fields

3. **`models/marts/marts.yml`** - Document your 6 mart models
   - 5 dimensions (dim_customer, dim_product, dim_store, dim_region, dim_date)
   - 1 fact (fact_orders)
   - Test all foreign key relationships

### 🔧 Required Setup

**Create `~/.dbt/profiles.yml`** (in your home directory)
- This is the ONLY file you need to create outside the project
- Contains PostgreSQL connection details
- See DBT_SETUP_GUIDE.md for template

## Your Data Flow

```
CSV Files → Python Cleaning → PostgreSQL (raw schema)
                                    ↓
                            dbt Staging (views)
                                    ↓
                            dbt Marts (tables)
```

## Expected Raw Tables (from load_to_db.py)

Based on your cleaning scripts, these tables will be in `raw` schema:

1. **customers** - customer_code, customer_segment, ...
2. **orders** - order_code, customer_code, store_code, product_code, order_status, order_type, ...
3. **products** - product_code, ...
4. **stores** - store_code, ...
5. **regions** - region attributes

## Next Steps Order

1. ✅ Load data to PostgreSQL (run `python/load_to_db.py`)
2. ✅ Create `~/.dbt/profiles.yml` for connection
3. ✅ Run `dbt debug` to test connection
4. ✅ Inspect raw tables to see actual columns
5. ✅ Populate `sources.yml` with discovered columns
6. ✅ Write staging SQL models (SELECT from sources)
7. ✅ Populate `staging.yml` with tests
8. ✅ Write mart SQL models (dimensions and facts)
9. ✅ Populate `marts.yml` with tests
10. ✅ Run `dbt run` and `dbt test`

## Key dbt Functions You'll Use

```sql
-- Reference source tables
SELECT * FROM {{ source('raw', 'customers') }}

-- Reference other models
SELECT * FROM {{ ref('stg_customers') }}

-- Model configuration
{{ config(materialized='table') }}

-- Variables (if needed)
{{ var('my_variable') }}
```

## Testing Strategy

**Start Simple:**
- Primary keys: `unique`, `not_null`
- Foreign keys: `relationships` test
- Categorical: `accepted_values` test

**Add Incrementally:**
- Don't test everything at once
- Test as you discover data issues
- Use `dbt test --select model_name` to test one model

## Common Issues & Solutions

**"Source not found"**
- Check `sources.yml` matches actual table names
- Verify schema name is correct (`raw`)

**"Relation does not exist"**
- Run `dbt run` to create the model first
- Check model dependencies (order matters)

**"Connection refused"**
- Verify PostgreSQL is running
- Check `profiles.yml` connection details
- Test with: `dbt debug`

## File Locations Summary

```
Project Root/
├── dbt_project.yml          ✅ Already configured
├── models/
│   ├── sources/
│   │   └── sources.yml      📝 Populate with raw tables
│   ├── staging/
│   │   ├── staging.yml      📝 Populate with staging docs
│   │   └── *.sql            ✅ Write your SQL here
│   └── marts/
│       ├── marts.yml        📝 Populate with mart docs
│       └── *.sql            ✅ Write your SQL here
└── ~/.dbt/
    └── profiles.yml         🔧 Create this file (connection config)
```

## Documentation Files Created

- **DBT_SETUP_GUIDE.md** - Detailed YAML setup instructions
- **setup.txt** - PostgreSQL connection guide for Cursor
- **DBT_QUICK_REFERENCE.md** - This file (quick overview)
