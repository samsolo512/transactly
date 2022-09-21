with
    src_tc_contact as(
        select *
        from {{ ref('src_tc_contact') }}
    )

    ,src_tc_party as(
        select *
        from {{ ref('src_tc_party') }}
    )

    ,src_tc_transaction_role as(
        select *
        from {{ ref('src_tc_transaction_role') }}
    )

select
    working.seq_dim_contact.nextval as contact_pk
    ,c.contact_id
    ,tr.role_name
    ,p.party_name
    ,c.side_id
    ,c.email
    ,c.last_name
    ,c.first_name
    ,c.transaction_id
from
    src_tc_contact c
    join src_tc_party p on c.party_id = p.party_id
    join src_tc_party s on c.side_id = s.party_id
    join src_tc_transaction_role tr on c.role_id = tr.role_id

union select 0, 0, null, null, null, null, null, null, null
