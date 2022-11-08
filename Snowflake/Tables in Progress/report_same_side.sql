with
    member1 as(
        select
            mc.*
            ,t.street
            ,t.city
            ,t.state
            ,t.zip
            ,t.diy_flag
            ,t.created_by_name
            ,t.created_by_id
            ,t.created_date
        from
            fact_transaction_member_contact fact
            join dim_transaction t on fact.transaction_pk = t.transaction_pk
            join dim_member_contact mc on fact.member_contact_pk = mc.member_contact_pk
    )

    ,member2 as(
        select
            m.*
        from
            fact_transaction_member_contact fact
            join dim_transaction t on fact.transaction_pk = t.transaction_pk
            join dim_member_contact m on fact.member_contact_pk = m.member_contact_pk
    )

    ,final as(
        select
            m1.transaction_id
            ,m1.street
            ,m1.city
            ,m1.state
            ,m1.zip
            ,m1.diy_flag
            ,m1.created_by_id
            ,m1.created_by_name
            ,m1.utility_opt_in_status
            ,m1.created_date as transaction_created_date

            ,m1.first_name as tc_first_name
            ,m1.last_name as tc_last_name
            ,m1.email as tc_email
            ,m1.role_name as tc_agent_role
            ,m1.party_name as tc_agent_side
            ,m2.first_name as agent_first_name
            ,m2.last_name as agent_last_name
            ,m2.email as agent_email
            ,m2.role_name as agent_role
            ,m2.party_name as agent_side
        from
            member1 m1
            join member2 m2 on m1.transaction_id = m2.transaction_id
    )

select distinct *
from final
where
    tc_agent_side = agent_side
    and tc_agent_role in('Seller Transaction Coordinator','Buyer Transaction Coordinator')
    and lower(agent_role) in('buyer agent','seller agent')
    and diy_flag = 1
    -- and transaction_id = 44331
;
