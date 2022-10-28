-- GCP_DIY_w_TC_on_same_side
-- this is just a version of GCP_fact_transaction_member but with 'where' statements and fewer 'select' columns

with
    fact_transaction_member as(
        select *
        from {{ ref('fact_transaction_member') }}
    )

    ,dim_user as(
        select *
        from {{ ref('dim_user') }}
    )

    ,dim_member as(
        select *
        from {{ ref('dim_member') }}
    )

    ,dim_transaction as(
        select *
        from {{ ref('dim_transaction') }}
    )

    ,final as(
        select
            t.street
            ,t.city
            ,t.state
            ,t.zip

            ,m.first_name as assigned_tc_first_name
            ,m.last_name as assigned_tc_last_name
            ,m.email as assigned_tc_email
            ,m.role_name as assigned_tc_role
            ,m.office_name as assigned_tc_office_name

            ,u.first_name as member_first_name
            ,u.last_name as member_last_name
            ,u.email as member_email

            ,t.side_id as transaction_side
            ,m.side_id as member_side

        from
            fact_transaction_member fact
            join dim_user u on fact.user_pk = u.user_pk
            join dim_member m on fact.member_pk = m.member_pk
            join dim_transaction t on fact.transaction_pk = t.transaction_pk

        where
            m.role_name in('Seller Transaction Coordinator','Buyer Transaction Coordinator')
            and t.diy_flag = 1
            and t.side_id = m.side_id
    )

select * from final