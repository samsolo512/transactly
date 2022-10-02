
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
            ,owner_name
            ,lead_partner_name
            ,contact_partner_name
            ,opportunity_partner_name
            ,lead_created_date
            ,contact_created_date
            ,opportunity_created_date
            ,opportunity_close_date
            ,opportunity_name
            ,stage
            ,agent_name
            ,agent_email

        from dim_lead
    )

select * from final
