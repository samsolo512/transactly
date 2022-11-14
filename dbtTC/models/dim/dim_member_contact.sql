with
    src_tc_transaction as(
        select *
        from {{ ref('src_tc_transaction') }}
    )

    ,src_tc_member as(
        select *
        from {{ ref('src_tc_member') }}
    )

    ,src_tc_contact as(
        select *
        from {{ ref('src_tc_contact') }}
    )

    ,src_tc_transaction_role as(
        select *
        from {{ ref('src_tc_transaction_role') }}
    )

    ,src_tc_user as(
        select *
        from {{ ref('src_tc_user') }}
    )

    ,dim_user as(
        select *
        from {{ ref('dim_user') }}
    )

    ,combine as(
        -- member
        select
            trans.transaction_id
            ,lower(u.email) as email
            ,m.member_id as member_contact_id
            ,'member' as member_or_contact
            ,u.first_name
            ,u.last_name
            ,u.phone
            ,m.role_id
            ,m.party_id
            ,1 as member_flag
            ,0 as contact_flag
            ,usr.utility_opt_in_status

        from
            src_tc_transaction trans
            join src_tc_member m on trans.transaction_id = m.transaction_id
            join src_tc_user u on m.user_id = u.user_id
            join dim_user usr on u.user_id = usr.user_id

        -- contact
        union all
        select
            trans.transaction_id
            ,lower(cont.email) as email
            ,cont.contact_id as member_contact_id
            ,'contact' as member_or_contact
            ,cont.first_name
            ,cont.last_name
            ,cont.phone
            ,cont.role_id
            ,cont.party_id
            ,0 as member_flag
            ,1 as contact_flag
            ,null as utility_opt_in_status

        from
            src_tc_transaction trans
            join src_tc_contact cont on trans.transaction_id = cont.transaction_id
    )

    ,final as(
        select
            working.seq_dim_member_contact.nextval as member_contact_pk
            ,c.member_contact_id
            ,c.transaction_id
            ,c.member_or_contact
            ,c.first_name
            ,c.last_name
            ,c.phone
            ,c.email
            ,c.role_id
            ,t.role_name
            ,c.party_id
            ,p.party_name
            ,c.member_flag
            ,c.contact_flag
            ,c.utility_opt_in_status

        from
            combine c
            left join src_tc_transaction_role t on c.role_id = t.role_id
            left join src_tc_party p on c.party_id = p.party_id
    )

select * from final
