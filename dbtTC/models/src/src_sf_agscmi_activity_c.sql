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
    ,agscmi_to_number_c as phone
    ,agscmi_chat_transcript_c as script
    ,agscmi_site_domain_c as website
    ,agscmi_campaign_c as campaign
    ,owner_id
    ,record_type_id
    ,agscmi_lead_c as lead_id
from 
    src_sf_agscmi_activity_c
