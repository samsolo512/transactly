{{ config(
    post_hook=[
      "{{unload_to_GCP('GCP_fact_text_chat')}}"
    ]
) }}


with
    dim_lead as(
        select *
        from {{ ref('dim_lead') }}
    )

    ,fact_text_chat as(
        select *
        from {{ ref('fact_text_chat') }}
    )

    ,final as(
        select
            fact.contact_method
            ,fact.created_date
            ,fact.caller_id
            ,fact.phone
            ,fact.direction
            ,fact.call_duration
            ,l.name as lead_owner_name
            ,l.owner_name as lead_owner

        from 
            fact_text_chat fact
            left join dim_lead l on fact.lead_pk = l.lead_pk
    )

select * from final
