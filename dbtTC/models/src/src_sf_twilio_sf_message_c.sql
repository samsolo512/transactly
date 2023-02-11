with src_sf_twilio_sf_message_c as(
    select *
    from {{ source('salesforce_salesforce', 'twilio_sf_message_c') }}
)

select
    owner_id
    ,name as message_id
     ,trim({{ field_clean('twilio_sf_body_c') }}) as text_body
    ,twilio_sf_to_number_unformatted_c as to_number
    ,twilio_sf_from_number_unformatted_c as from_number
    ,cast(twilio_sf_date_created_c as date) as created_date
    ,twilio_sf_direction_c as direction
    ,response_time_c as response_time
from 
    src_sf_twilio_sf_message_c
where
    is_deleted = FALSE
