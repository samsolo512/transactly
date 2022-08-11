with src_sf_contact as(
    select *
    from fivetran.salesforce.contact
)

select
    c.agent_c
    ,c.agent_brokerage_c
    ,c.id as contact_id
    ,c.mailing_street as street
    ,c.mailing_state as state
from src_sf_contact c
