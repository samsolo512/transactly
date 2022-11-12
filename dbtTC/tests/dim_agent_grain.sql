-- dim_agent_grain
-- 1 row/ agent

select 
    agent_email, count(1) 
from  
    {{ ref('dim_agent') }} 
group by 
    agent_email
having 
    count(1) > 1