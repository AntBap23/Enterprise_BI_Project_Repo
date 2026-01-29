select 
    store_code::text as store_code,
    store_name::text as store_name,
    city::text as city,
    state::text as state,
    region_name::text as region_name,
    open_date::date as open_date,
    close_date::date as close_date,
    square_feet::float as square_feet
from {{ source('raw', 'stores') }}
where store_code is not null
