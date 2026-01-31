{{ config(materialized='table') }}

with base as (
    select
        region_name
    from {{ ref('stg_regions') }}
    where region_name is not null
    group by region_name
)

select
    row_number() over (order by region_name) as region_key,
    region_name::text                         as region_name
from base

