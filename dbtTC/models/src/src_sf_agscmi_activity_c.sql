with src_sf_agscmi_activity_c as(
    select *
    from {{ source('salesforce_salesforce', 'agscmi_activity_c') }}
)

select
    direction_c as direction
    ,call_twilio_client_c as call_twilio_client
    ,name as activity_name
    ,agscmi_external_id_c as external_id
    ,agscmi_caller_id_c as caller_id
    ,call_duration_in_seconds_c as call_duration_in_seconds
    ,to_date(created_date) as created_date
from 
    src_sf_agscmi_activity_c
