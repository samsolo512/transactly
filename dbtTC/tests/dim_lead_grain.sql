-- dim_lead_grain
-- 1 row/ contact

select 
    lead_id, count(1) 
from  
    {{ ref('dim_lead') }} 
group by 
    lead_id
having 
    count(1) > 1
