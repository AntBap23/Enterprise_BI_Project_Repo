{{ config(materialized='table') }}

select
    {{ ref('stg_stores') }}.store_code::text as store_code,
    {{ ref('stg_stores') }}.store_name::text as store_name,
    {{ ref('stg_stores') }}.city::text as city,
    {{ ref('stg_stores') }}.state::text as state,
    {{ ref('dim_region') }}.region_name::text as region_name,
    {{ ref('stg_stores') }}.open_date::date as open_date,
    {{ ref('stg_stores') }}.close_date::date as close_date,
    {{ ref('stg_stores') }}.square_feet::float as square_feet
from {{ ref('stg_stores') }}
join {{ ref('dim_region') }} on {{ ref('stg_stores') }}.region_name = {{ ref('dim_region') }}.region_name