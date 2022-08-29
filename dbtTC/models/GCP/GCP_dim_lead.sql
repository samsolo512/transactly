
{{ config(
    post_hook=[
      "{{unload_dim_lead_to_GCP()}}"
    ]
) }}

with
    dim_lead as(
        select *
        from {{ ref('dim_lead') }}
    )

    ,final as(
        select
            first_name
            ,last_name
            ,name
            ,company
            ,street
            ,city
            ,state
            ,postal_code
            ,country
            ,mobile_phone
            ,email
            ,lead_source
            ,created_date
            ,owner_first_name
            ,owner_last_name
            ,owner_name
            ,owner_title
            ,owner_street
            ,owner_city
            ,owner_postal_code
            ,owner_country
            ,owner_email
            ,owner_phone
            ,owner_mobile_phone
            ,owner_is_active_flag
        
        from dim_lead
    )

select * from final
