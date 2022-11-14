-- dim_member_contact
-- 1 row/ contact

select 
    transaction_id, member_contact_id, member_or_contact, count(1) 
from  
    {{ ref('dim_member_contact') }} 
group by 
    transaction_id, member_contact_id, member_or_contact
having 
    count(1) > 1
