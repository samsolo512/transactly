-- dim_member_grain
-- 1 row/ member

select 
    member_id, count(1) 
from  
    {{ ref('dim_member') }} 
group by 
    member_id
having 
    count(1) > 1