{{ config(
    post_hook=[
      "{{unload_to_GCP('GCP_fact_call_chat')}}"
    ]
) }}


with
    dim_lead as(
        select *
        from {{ ref('dim_lead') }}
    )

    ,fact_call_chat as(
        select *
        from {{ ref('fact_call_chat') }}
    )

    ,final as(
        select
            fact.contact_method
            ,fact.created_date
            ,fact.caller_id
            ,fact.phone
            ,fact.direction
            ,call_duration_in_seconds
            ,fact.call_duration
            ,l.name as lead_name
            ,l.owner_name as lead_owner
            ,call_twilio_client
            ,activity_name
            
        from 
            fact_call_chat fact
            left join dim_lead l on fact.lead_pk = l.lead_pk
    )

select * from final
