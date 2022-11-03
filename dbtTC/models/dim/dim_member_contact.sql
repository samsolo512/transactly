with
    src_tc_transaction as(
        select *
        from {{ ref('src_tc_transaction') }}
    )

    ,dim_transaction as(
        select *
        from {{ ref('dim_transaction') }}
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

    ,combine as(
        -- member
        select
            trans.transaction_id
            ,lower(u.email) as email
            ,m.member_id as member_contact_id
            ,u.first_name
            ,u.last_name
            ,u.phone
            ,m.role_id
            ,1 as member_flag
            ,0 as contact_flag

        from
            src_tc_transaction trans
            join src_tc_member m on trans.transaction_id = m.transaction_id
            join src_tc_user u on m.user_id = u.user_id

        -- contact
        union all
        select
            trans.transaction_id
            ,lower(cont.email) as email
            ,cont.contact_id as member_contact_id
            ,cont.first_name
            ,cont.last_name
            ,cont.phone
            ,cont.role_id
            ,0 as member_flag
            ,1 as contact_flag

        from
            src_tc_transaction trans
            join src_tc_contact cont on trans.transaction_id = cont.transaction_id
    )

    ,final as(
        select
            working.seq_dim_member_contact.nextval as member_contact_pk
            ,c.member_contact_id
            ,c.transaction_id
            ,c.first_name
            ,c.last_name
            ,c.phone
            ,c.email
            ,t.role_name
            ,c.member_flag
            ,c.contact_flag

        from
            combine c
            join src_tc_transaction_role t on c.role_id = t.role_id
    )

select * from final
