{{ config(materialized='table') }}

with base as (
    select 
        customer_code::text as customer_code,
        signup_date::date as signup_date,
        customer_segment::text as customer_segment,
        email::text as email,
        loyalty_id::text as loyalty_id,
        row_number() over (partition by customer_code order by customer_code) as rn
    from {{ ref('stg_customers') }}
    where customer_code is not null
)

select
    customer_code,
    signup_date,
    customer_segment,
    email,
    loyalty_id
from base
where rn = 1

