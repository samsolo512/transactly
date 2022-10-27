-- GCP_user_agent_difference
-- this is just a version of GCP_fact_order using 'where' statements and fewer 'select' columns

{{ config(
    post_hook=[
      "{{unload_to_GCP('GCP_user_agent_difference')}}"
    ]
) }}

with
    fact_order as(
        select *
        from {{ ref('fact_order') }}
    )

    ,dim_order as(
        select *
        from {{ ref('dim_order') }}
    )

    ,dim_user as(
        select *
        from {{ ref('dim_user') }}
    )

    ,dim_transaction as(
        select *
        from {{ ref('dim_transaction') }}
    )

    ,dim_date as(
        select *
        from {{ ref('dim_date') }}
    )

    ,final as(
        select
            t.street as transaction_street
            ,t.city as transaction_city
            ,t.state as transaction_state
            ,t.zip as transaction_zip
            ,u.first_name as user_first_name
            ,u.last_name as user_last_name
            ,u.email as user_email
            ,o.office_name
            ,ua.first_name as assigned_tc_first_name
            ,ua.last_name as assigned_tc_last_name
            ,ua.email as assigned_tc_email
            ,uc.first_name as created_by_first_name
            ,uc.last_name as created_by_last_name
            ,uc.email as created_by_email
        from
            fact_order fact
            join dim_order o on fact.order_pk = o.order_pk
            join dim_transaction t on fact.transaction_pk = t.transaction_pk
            join dim_user ua on fact.assigned_tc_pk = ua.user_pk
            join dim_user uc on fact.created_by_pk = uc.user_pk
            join dim_user u on fact.user_pk = u.user_pk
            left join dim_date closed_date on fact.closed_date_pk = closed_date.date_pk
        where
            u.user_pk <> uc.user_pk
            and(
                nvl(o.office_name, 'a') not in(
                    'TC Solutions Clients'
                    ,'The NOMA Group'
                    ,'NOMA Group Corporate'
                    ,'Noma-Venture REI'
                    ,'The Noma Group - NV'
                    ,'Renters Warehouse'
                    ,'Renters Warehouse - GA'
                    ,'Renters Warehouse - AZ'
                    ,'Renters Warehouse - NV'
                    ,'Renters Warehouse - FL'
                    ,'Renters Warehouse - NC'
                    ,'Moncord'
                )
            )
    )

select * from final
