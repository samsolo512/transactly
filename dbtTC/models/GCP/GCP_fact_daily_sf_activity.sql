{{ config(
    post_hook=[
      "{{unload_to_GCP('GCP_fact_daily_sf_activity')}}"
    ]
) }}


with
    fact_daily_sf_activity as(
        select *
        from {{ ref('fact_daily_sf_activity') }}
    )

    ,final as(
        select
            date
            ,leads_created
            ,leads_converted
            ,opportunities_created
            ,opportunities_created_amount
            ,opportunities_won
            ,opportunities_won_amount
            ,opportunities_lost
            ,opportunities_lost_amount
            ,pipeline_amount

        from fact_daily_sf_activity
    )

select * from final
