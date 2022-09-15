
{{ config(
    post_hook=[
      "{{unload_to_GCP('GCP_fact_transaction_member')}}"
    ]
) }}

with
    fact_transaction_member as(
        select *
        from {{ ref('fact_transaction_member') }}
    )

    ,dim_transaction as(
        select *
        from {{ ref('dim_transaction') }}
    )

    ,dim_user as(
        select *
        from {{ ref('dim_user') }}
    )

    ,dim_member as(
        select *
        from {{ ref('dim_member') }}
    )

    ,dim_date as(
        select *
        from {{ ref('dim_date') }}
    )

    ,final as (
        select
--                 top 10000
--                 top 1000
--                 top 500
--                 top 250
--                 top 350
--                 top 300  -- failed
--                 top 294  -- worked
            t.street
            ,t.city
            ,t.state
            ,t.zip
            ,u.first_name as member_first_name
            ,u.last_name as member_last_name
            ,u.email as member_email
            ,fact.tc_buyer_as_connect_lead_flag
            ,dt.date_id as connect_lead_created_date

        from
            fact_transaction_member fact
            join dim_user u on fact.user_pk = u.user_pk
            join dim_member m on fact.member_pk = m.member_pk
            join dim_transaction t on fact.transaction_pk = t.transaction_pk
            join dim_date dt on fact.connect_lead_created_date_pk = dt.date_pk

        where
            street not like '%><img src=x onerror=prompt(1)>%'
        
        order by t.street, u.email
    )

select * from final