-- hs_agent
-- 1 row/agent

select
    transactly_id
    ,count(1) 
from {{ ref('hs_agent') }}
where transactly_id is not null
group by all 
having count(1) > 1
