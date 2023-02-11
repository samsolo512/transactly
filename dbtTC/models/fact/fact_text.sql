-- fact_text
-- 1 row/communication event

with
    src_sf_twilio_sf_message_c as(
        select *
        from {{ ref('src_sf_twilio_sf_message_c')}}
    )

    ,src_sf_user as(
        select *
        from {{ ref('src_sf_user')}}
    )

    ,final as(
        select
            u.name as user_name
            ,a.message_id
            ,a.text_body
            ,a.to_number
            ,a.from_number
            ,a.created_date
            ,a.direction
            ,a.response_time
        from 
            src_sf_twilio_sf_message_c a
            join src_sf_user u on a.owner_id = u.user_id
    )

select * from final
