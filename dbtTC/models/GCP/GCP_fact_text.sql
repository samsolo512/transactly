{{ config(
    post_hook=[
      "{{unload_to_GCP('GCP_fact_text')}}"
    ]
) }}


with
    fact_text as(
        select *
        from {{ ref('fact_text') }}
    )

    ,final as(
        select
            user_name
            ,message_id
            ,text_body
            ,to_number
            ,from_number
            ,created_date
            ,direction
            ,response_time
            
        from 
            fact_text fact
    )

select * from final
