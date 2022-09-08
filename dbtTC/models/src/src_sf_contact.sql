with src_sf_contact as(
    select *
    from {{ source('sf', 'contact') }}
)

select
    c.agent_c
    ,c.agent_brokerage_c
    ,c.id as contact_id
    ,replace(
        replace(
            replace(c.mailing_street, '"', ''),
            char(10),
            ' '
        ),
        char(13),
        ' '
    ) as street
    ,c.mailing_state as state
from src_sf_contact c
where c.is_deleted = 'FALSE'
