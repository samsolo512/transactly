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
            ,partner
            ,electricity
            ,sewer
            ,trash
            ,water
            ,gas
            ,internet
            ,contact_owner

--             ,owner_name
--             ,lead_partner_name
--             ,contact_partner_name
--             ,opportunity_partner_name
--             ,contact_created_date
--             ,opportunity_created_date
--             ,opportunity_close_date
--             ,opportunity_name
--             ,stage

        from dim_lead
    )

select * from final
