-- fact_daily_sf_activity
-- 1 row/date

-- fact_daily_sf_activity

with
    src_sf_opportunity as(
        select *
        from {{ ref('src_sf_opportunity')}}
    )
    
    ,dim_date as(
        select *
        from {{ ref('dim_date')}}
    )
    
    ,src_sf_lead as(
        select *
        from {{ ref('src_sf_lead')}}
    )
    
    ,lead_created as(
        select
            created_date
            ,count(1) as leads_created
        from src_sf_lead
        group by created_date
    )
    
    ,lead_converted as(
        select
            converted_date
            ,count(1) as leads_converted
        from src_sf_lead
        where
            is_converted
        group by converted_date
    )
    
    ,opportunity_created as(
        select
            created_date
            ,count(opportunity_id) as opportunities_created
            ,round(sum(amount)) as opportunities_created_amount
        from src_sf_opportunity
        group by created_date
    )
    
    ,opportunity_closed as (
        select
            close_date,
            count(case when is_won then opportunity_id else null end) as opportunities_won,
            round(sum(case when is_won then amount else 0 end)) as opportunities_won_amount,
            count(case when not is_won and is_closed then opportunity_id else null end) as opportunities_lost,
            round(sum(case when not is_won and is_closed then amount else null end)) as opportunities_lost_amount,
            round(sum(
                case 
                    when is_closed and lower(forecast_category) in ('pipeline','forecast','bestcase') 
                    then amount 
                    else null 
                    end
            )) as pipeline_amount
        from src_sf_opportunity
        group by 1
    )
    
    ,final as(
        select
            dt.date_pk

            --grain
            ,to_date(a.created_date) as date

            -- measures
            ,a.leads_created
            ,b.leads_converted
            ,c.opportunities_created
            ,c.opportunities_created_amount
            ,d.opportunities_won
            ,d.opportunities_won_amount
            ,d.opportunities_lost
            ,d.opportunities_lost_amount
            ,d.pipeline_amount

        from
            lead_created a
            left join lead_converted b on a.created_date = b.converted_date
            left join opportunity_created c on a.created_date = c.created_date
            left join opportunity_closed d on a.created_date = d.close_date
            left join dim_date dt on a.created_date = dt.date_id
    )
    
select * from final order by date desc
