-- dim_task_grain
-- 1 row/ task

select 
    task_id, count(1) 
from  
    {{ ref('dim_task') }} 
group by 
    task_id
having 
    count(1) > 1