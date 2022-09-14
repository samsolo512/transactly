with
    src_tc_member as(
        select *
        from {{ ref('src_tc_member') }}
    )

--     ,src_tc_transaction_role as(
--         select *
--         from {{ ref('src_tc_transaction_role') }}
--     )

    ,dim_user as(
        select *
        from {{ ref('dim_user') }}
    )

    ,dim_transaction as(
        select *
        from {{ ref('dim_transaction') }}
    )

--     ,src_tc_party as(
--         select *
--         from {{ ref('src_tc_party') }}
--     )

    ,dim_member as(
        select *
        from {{ ref('dim_member') }}
    )

    ,final as(
        select
            memb.member_pk
            ,tc_u.user_pk
            ,dt.transaction_pk

            ,case when memb.role_name in('Buyer') and dl.email is not null then c.date_pk else 0 end as connect_lead_created_date_pk

            ,case when memb.role_name in('Buyer') and dl.email is not null then 1 else 0 end as TC_buyer_as_Connect_lead_flag

        from
            src_tc_member m
--             join src_tc_transaction_role tr on m.role_id = tr.role_id
            join dim_user tc_u on m.user_id = tc_u.user_id
--             join src_tc_party p on m.party_id = p.party_id
            join dim_member memb on m.member_id = memb.member_id
            join dim_transaction dt on m.transaction_id = dt.transaction_id
            left join dim_lead dl on tc_u.email = dl.email
            left join dim_date c on dl.created_date = c.date_id
    )

select * from final
