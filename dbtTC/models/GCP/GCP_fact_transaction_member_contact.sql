
{{ config(
    post_hook=[
      "{{unload_to_GCP('GCP_fact_transaction_member_contact')}}"
    ]
) }}

with
    fact_transaction_member_contact as(
        select *
        from {{ ref('fact_transaction_member_contact') }}
    )

    ,dim_transaction as(
        select *
        from {{ ref('dim_transaction') }}
    )

    ,dim_member_contact as(
        select *
        from {{ ref('dim_member_contact') }}
    )

    ,final as (
        select
            -- transaction
            t.transaction_id
            ,t.street
            ,t.city
            ,t.state
            ,t.zip

            -- agent
            ,t.agent_first_name
            ,t.agent_last_name
            ,t.agent_email
            ,t.agent_phone

            -- tc_agent
            ,t.tc_agent_first_name
            ,t.tc_agent_last_name
            ,t.tc_agent_email
            ,t.tc_agent_phone

            -- member_contact
            ,mc.first_name as member_contact_first_name
            ,mc.last_name as member_contact_last_name
            ,mc.role_name as member_contact_role
            ,mc.phone as member_contact_phone
            ,mc.email as member_contact_email
            ,mc.member_flag
            ,mc.contact_flag

            -- transaction
            --,t.side_id as transaction_side
            ,t.order_side
            ,t.status
            ,t.diy_flag
            ,t.contract_closing_date
            ,t.closed_date transaction_closed_date

            -- fact
            ,fact.utility_transfer_status
            ,fact.utility_lead_sent_to
            ,fact.utility_notified_date
            ,fact.home_insurance_status
            ,fact.home_insurance_lead_sent_to
            ,fact.home_insurance_notified_date

        from
            fact_transaction_member_contact fact
            join dim_transaction t on fact.transaction_pk = t.transaction_pk
            left join dim_member_contact mc
                on t.transaction_id = mc.transaction_id
                and fact.member_contact_pk = mc.member_contact_pk

    )

select * from final