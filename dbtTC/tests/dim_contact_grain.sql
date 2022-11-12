-- dim_contact_grain
-- 1 row/ contact

select 
    contact_id, count(1) 
from  
    {{ ref('dim_contact') }} 
group by 
    contact_id
having 
    count(1) > 1
