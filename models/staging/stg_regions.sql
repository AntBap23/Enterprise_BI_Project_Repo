select 
    region_name::text as region_name,
    active_flag::text as active_flag,
    region_code::text as region_code
from {{ source('raw', 'regions') }}
where region_name is not null