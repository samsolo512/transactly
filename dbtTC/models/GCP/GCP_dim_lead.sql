{{ config(
    post_hook=[
      "{{unload_to_GCP('GCP_dim_lead')}}"
    ]
) }}


with
    dim_lead as(
        select *
        from {{ ref('dim_lead') }}
    )

    ,final as(
        select
            lead_id
            ,first_name
            ,last_name
            ,name
            ,company
            ,street
            ,city
            ,state
            ,zip
            ,country
            ,full_address
            ,phone
            ,email
            ,lead_source
            ,lead_created_date
            ,agent_name
            ,agent_email
            ,electricity
            ,sewer
            ,trash
            ,water
            ,gas
            ,internet
            ,account_name
            ,parent_account_name
            ,account_owner
            ,lead_week_date
            ,owner_name as lead_owner_name
            ,status as lead_status
            ,attribution

        from dim_lead
    )

select * from final
