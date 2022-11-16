-- dim_opportunity_grain
-- 1 row/ opportunity

select 
    opportunity_id, product_id, count(1) 
from  
    {{ ref('dim_opportunity') }} 
group by 
    opportunity_id, product_id
having 
    count(1) > 1