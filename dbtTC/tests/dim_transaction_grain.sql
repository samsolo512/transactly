-- dim_transaction_grain
-- 1 row/ transaction

select 
    transaction_id, count(1) 
from  
    {{ ref('dim_transaction') }} 
group by 
    transaction_id
having 
    count(1) > 1