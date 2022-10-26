-- fact_opportunity
-- 1 row/opportunity

select
    fact.*
    ,o.opportunity_name
    ,o.opportunity_line_item_name
from
    fact_opportunity fact
    join dim_lead l on fact.lead_pk = l.lead_pk
    join dim_opportunity o on fact.opportunity_pk = o.opportunity_pk
    join dim_agent a on fact.agent_pk = a.agent_pk
-- where
--     o.opportunity_name like '%Tamika Owens 2022%'
;