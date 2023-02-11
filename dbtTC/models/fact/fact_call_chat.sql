-- fact_call_chat
-- 1 row/communication event

with
    src_sf_agscmi_activity_c as(
        select *
        from {{ ref('src_sf_agscmi_activity_c')}}
    )

    ,src_sf_record_type as(
        select *
        from {{ ref('src_sf_record_type')}}
    )

    ,dim_lead as(
        select *
        from {{ ref('dim_lead')}}
    )

    ,prep as(
        select
            case
                when lower(b.record_type_name) = 'sms' then 'Text'
                else b.record_type_name
                end as contact_method
            ,a.created_date
            ,a.caller_id
            ,a.phone
            ,a.direction
            ,a.call_duration_in_seconds  -- desc table src_sf_agscmi_activity_c
            ,case 
                when a.call_duration_in_seconds/3600 > 1 then floor(a.call_duration_in_seconds/3600, 0) 
                else null 
                end as hours
            ,case 
                when a.call_duration_in_seconds/60 between 1 and 59 then floor(a.call_duration_in_seconds/60, 0) 
                else null 
                end as minutes
            ,case 
                when a.call_duration_in_seconds < 60 then a.call_duration_in_seconds
                when a.call_duration_in_seconds >= 60 then a.call_duration_in_seconds%60
                else null 
                end as seconds
            ,a.lead_id
            ,a.call_twilio_client
            ,a.activity_name
        from 
            src_sf_agscmi_activity_c a
            join src_sf_record_type b on a.record_type_id = b.record_type_id
    )
    
    ,final as(
        select
            l.lead_pk
            ,p.contact_method
            ,p.created_date
            ,p.caller_id
            ,p.phone
            ,p.direction
            ,p.call_duration_in_seconds
            ,concat(lpad(nvl(hours, '0'),2,0), ':', lpad(nvl(minutes, '0'),2,0), ':', lpad(nvl(seconds, '0'),2,0)) as call_duration
            ,p.call_twilio_client
            ,p.activity_name
        from 
            prep p
            left join dim_lead l on p.lead_id = l.lead_id
    )

select * from final
