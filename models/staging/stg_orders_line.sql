{{ config(materialized='view') }}

select
  order_code::text        as order_id,
  line_number::int        as line_number,

  store_code::text        as store_id,
  customer_code::text     as customer_id,
  product_code::text      as product_id,

  order_ts::date          as order_date,
  delivery_ts::date       as delivery_date,

  order_type::text        as order_type,
  order_status::text      as order_status,

  quantity::int           as quantity,
  unit_price::numeric     as unit_price,
  unit_cost::numeric      as unit_cost,

  (quantity::numeric * unit_price::numeric) as line_revenue,
  (quantity::numeric * unit_cost::numeric)  as line_cost,
  (quantity::numeric * (unit_price::numeric - unit_cost::numeric)) as line_margin

from {{ source('raw', 'orders') }}
where order_code is not null
  and line_number is not null
