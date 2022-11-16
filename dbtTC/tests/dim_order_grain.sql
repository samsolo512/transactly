-- dim_order_grain
-- 1 row/ order

select 
    order_id, count(1) 
from  
    {{ ref('dim_order') }} 
group by 
    order_id
having 
    count(1) > 1