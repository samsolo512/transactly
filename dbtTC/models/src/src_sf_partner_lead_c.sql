with partner_lead_c as(
    select *
    from {{ source('salesforce_salesforce', 'partner_lead_c') }}
)

select
    p.id
    ,p.lead_c
    ,p.partner_c
    ,p.created_date
from partner_lead_c p
where
    is_deleted = 'FALSE'