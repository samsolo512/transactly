
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
            first_name
            ,last_name
            ,name
            ,company
            ,street
            ,city
            ,state
            ,postal_code
            ,country
            ,full_address
            ,mobile_phone
            ,email
            ,lead_source
            ,created_date
            ,owner_name

        from dim_lead
    )

select * from final
