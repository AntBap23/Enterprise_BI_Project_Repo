select 
    product_code::text as product_code,
    product_name::text as product_name,
    category::text as category,
    base_price::float as base_price,
    active_flag::text as active_flag
from {{ source('raw', 'products') }}
where product_code is not null