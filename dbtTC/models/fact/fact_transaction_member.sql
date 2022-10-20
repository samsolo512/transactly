with
    src_tc_member as(
        select *
        from {{ ref('src_tc_member') }}
    )

    ,dim_user as(
        select *
        from {{ ref('dim_user') }}
    )

    ,dim_transaction as(
        select *
        from {{ ref('dim_transaction') }}
    )

    ,dim_member as(
        select *
        from {{ ref('dim_member') }}
    )

    ,dim_contact as(
        select *
        from {{ ref('dim_contact') }}
    )

    ,final as(
        select
            dt.transaction_pk
            ,nvl(u.user_pk, 0) as user_pk
            ,nvl(memb.member_pk, 0) as member_pk
--             ,nvl(cont.contact_pk, 0) as contact_pk

            ,case
                when memb.role_name in('Buyer') and dl.email is not null then c.date_pk
                else 0
                end as connect_lead_created_date_pk
            ,case when memb.role_name in('Buyer') and dl.email is not null then 1
                else 0
                end as TC_buyer_as_Connect_lead_flag
            ,dt.transaction_id

        from
            src_tc_transaction trans
            join dim_transaction dt on trans.transaction_id = dt.transaction_id
            left join dim_user u on trans.created_by_id = u.user_id
            left join src_tc_member m on trans.transaction_id = m.transaction_id
            left join dim_member memb on m.member_id = memb.member_id
            left join(
                select
                    max(lead_created_date) as lead_created_date
                    ,email
                from dim_lead
                group by email
            ) dl on memb.email = dl.email
            left join dim_date c on dl.lead_created_date = c.date_id
--             left join dim_contact cont on trans.transaction_id = cont.transaction_id
    )

select * from final
