{{ config(materialized='table') }}

select 
    customer_code::text as customer_code,
    signup_date::date as signup_date,
    customer_segment::text as customer_segment,
    email::text as email,
    loyalty_id::text as loyalty_id
from {{ ref('stg_customers') }}
where customer_code is not null and is_unique(customer_code)


