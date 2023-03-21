{{ config(
    post_hook=[
      "{{unload_to_GCP('GCP_fact_call_text')}}"
    ]
) }}


with
    fact_call_text as(
        select *
        from {{ ref('fact_call_text') }}
    )

    ,final as(
        select
            fact.contact_method
            ,fact.created_date
            ,fact.caller_id
            ,fact.phone
            ,fact.direction
            ,fact.call_duration_in_seconds
            ,fact.call_duration
            ,fact.user
            ,fact.activity_name
            ,fact.source
            ,fact.message
            ,fact.message_id
            ,fact.response_time
        from 
            fact_call_text fact
    )

select * from final
