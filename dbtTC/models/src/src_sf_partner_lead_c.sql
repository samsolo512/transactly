with partner_lead_c as(
    select *
    from {{ source('sf', 'partner_lead_c') }}
)

select
    p.id
    ,p.lead_c
    ,p.partner_c
from partner_lead_c p
where
    is_deleted = 'FALSE'