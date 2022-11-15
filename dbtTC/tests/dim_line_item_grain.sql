-- dim_line_item_grain
-- 1 row/ line item

select 
    line_item_id, count(1) 
from  
    {{ ref('dim_line_item') }} 
group by 
    line_item_id
having 
    count(1) > 1
