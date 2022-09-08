with
    src_tc_member as(
        select *
        from {{ ref('src_tc_member') }}
    )

    ,src_tc_transaction_role as(
        select *
        from {{ ref('src_tc_transaction_role') }}
    )

    ,src_tc_party as(
        select *
        from {{ ref('src_tc_party') }}
    )

    ,final as(
        select
            working.seq_dim_member.nextval as member_pk
            ,m.member_id
            ,tr.role_name
            ,p.party_name

        from
            src_tc_member m
            join src_tc_transaction_role tr on m.role_id = tr.role_id
            join src_tc_party p on m.party_id = p.party_id

        union select 0, 0, null, null
    )

select * from final
