with orders as (

select 
	order_code,
	customer_code,
	store_code,
	order_ts,
	delivery_ts,
	order_revenue,
	order_cost,
	order_margin,
	line_count,
	total_units,
	order_status,
	order_type
from staging_mart.fct_orders

)

,customers as (

select
	customer_code,
	signup_date,
	customer_segment,
	email,
	loyalty_id
from staging_mart.dim_customer

)


,stores as (

select
	store_code,
	store_name,
	city,
	state,
	region_name,
	open_date,
	close_date,
	square_feet
from staging_mart.dim_store

)


,regions as (

select
	region_name
from staging_mart.dim_region

)

,dates as (

select
	date_day,
	year,
	month,
	month_name,
	year_month,
	quarter,
	week_of_year,
	day_of_month,
	day_name,
	is_weekend
from staging_mart.dim_date

)




select 
	o.*,
	c.signup_date,
	c.customer_segment,
	c.email,
	c.loyalty_id,
	s.store_name,
	s.city,
	s.state,
	s.open_date,
	s.close_date,
	s.square_feet,
	r.region_name,
	d.*
from orders o
left join customers c on c.customer_code = o.customer_code
left join stores s on s.store_code = o.store_code
left join regions r on r.region_name = s.region_name
left join dates d on d.date_day = o.order_ts

	