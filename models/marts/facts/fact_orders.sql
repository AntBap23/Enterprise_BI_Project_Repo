{{ config(materialized='table') }}

with lines as (
    select
        order_id      as order_code,
        customer_id   as customer_code,
        store_id      as store_code,

        order_date    as order_ts,
        delivery_date as delivery_ts,
        order_status,
        order_type,

        quantity,
        unit_price,
        unit_cost
    from {{ ref('stg_orders_line') }}
)

select
    order_code,

    -- dimensions / keys for slicing
    customer_code,
    store_code,

    -- timestamps (choose a rule; min/max is safe)
    min(order_ts)    as order_ts,
    max(delivery_ts) as delivery_ts,

    -- order-level metrics (sum across lines)
    sum(quantity * unit_price)                        as order_revenue,
    sum(quantity * unit_cost)                         as order_cost,
    sum(quantity * (unit_price - unit_cost))          as order_margin,
    count(*)                                          as line_count,
    sum(quantity)                                     as total_units,

    -- status/type: if consistent per order, max() is fine
    max(order_status) as order_status,
    max(order_type)   as order_type

from lines
group by 1,2,3
