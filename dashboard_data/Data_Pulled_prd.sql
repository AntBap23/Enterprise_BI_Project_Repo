select 
	o.*,
	p.product_name,
	p.category,
	p.base_price,
	p.active_flag
from staging_mart.fact_orders o
left join staging_mart.dim_product p on p.product_code = o.product_code
