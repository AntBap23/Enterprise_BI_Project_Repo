{{ config(materialized='table') }}

with bounds as (
    select
        min(order_date::date) as start_date,
        max(order_date::date) as end_date
    from {{ ref('stg_orders_line') }}
),

date_spine as (
    select generate_series(
        (select start_date from bounds),
        (select end_date from bounds),
        interval '1 day'
    )::date as date_day
)

select
    date_day,

    extract(year from date_day)::int as year,
    extract(month from date_day)::int as month,
    to_char(date_day, 'Mon') as month_name,

    to_char(date_day, 'YYYY-MM') as year_month,

    extract(quarter from date_day)::int as quarter,
    extract(week from date_day)::int as week_of_year,

    extract(day from date_day)::int as day_of_month,
    to_char(date_day, 'Dy') as day_name,

    case when extract(isodow from date_day) in (6,7) then true else false end as is_weekend
from date_spine
order by date_day
