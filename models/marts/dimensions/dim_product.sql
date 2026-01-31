{{ config(materialized='table') }}

with base as (
    select 
        product_code::text as product_code,
        product_name::text as product_name,
        category::text as category,
        base_price::float as base_price,
        active_flag::text as active_flag,
        row_number() over (partition by product_code order by product_code) as rn
    from {{ ref('stg_products') }}
    where product_code is not null
)

select
    product_code,
    product_name,
    category,
    base_price,
    active_flag
from base
where rn = 1