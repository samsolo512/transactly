-- fact_text
-- 1 row/communication event

with
    src_hs_object_properties as(
        select *
        from {{ ref('src_hs_object_properties')}}
    )

    ,src_hs_owners as(
        select *
        from {{ ref('src_hs_owners')}}
    )

    ,src_sf_agscmi_activity_c as(
        select *
        from {{ ref('src_sf_agscmi_activity_c')}}
    )

    ,src_sf_record_type as(
        select *
        from {{ ref('src_sf_record_type')}}
    )

    ,src_sf_twilio_sf_message_c as(
        select *
        from {{ ref('src_sf_twilio_sf_message_c')}}
    )

    ,src_sf_user as(
        select *
        from {{ ref('src_sf_user')}}
    )

    ,dim_lead as(
        select *
        from {{ ref('dim_lead')}}
    )

    ,sf as(
        select
            case
                when lower(b.record_type_name) = 'sms' then 'Text'
                else b.record_type_name
                end as contact_method
            ,a.created_date
            ,regexp_replace(a.caller_id, '[\\+\\-\\)\\(]') as caller_id
            ,regexp_replace(a.phone, '[\\+\\-\\)\\(]') as phone
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

    ,allcomm as(
        select
            objectid
            ,value as contact_method
        from 
            src_hs_object_properties
        where
            lower(name) = 'hs_engagement_type'
            and value in('SMS', 'CALL')
    )

    ,rawdata as(
        select
            o.objecttypeid
            ,o.objectid
            ,case
                when lower(a.contact_method) = 'call' then 'Call'
                when lower(a.contact_method) = 'sms' then 'Text'
                else a.contact_method
                end as contact_method
            ,o.name
            ,o.value
            ,case 
                when a.contact_method = 'SMS' and left(o.value, 8) in('Incoming', 'Outgoing') then left(o.value, 8)
                when a.contact_method = 'CALL' and o.value like ('% Outgoing %') then 'Outbound'
                when a.contact_method = 'CALL' then replace(replace(regexp_substr(o.value, '\\[.{1,3}bound\\]'), '['), ']')
                else null 
                end as message_direction
            ,case 
                when a.contact_method = 'SMS' and left(o.value, 8) in('Incoming', 'Outgoing') then substr(o.value, regexp_instr(o.value, '[0-9]{11}', 1, 1), 11) 
                when a.contact_method = 'CALL' then substr(regexp_substr(o.value, '\\+[0-9]{11}'), 2, 13)
                else null 
                end as from_number
            ,case 
                when a.contact_method = 'SMS' and left(o.value, 8) in('Incoming', 'Outgoing') then substr(o.value, regexp_instr(o.value, '[0-9]{11}', 1, 2), 11)
                when a.contact_method = 'CALL' then substr(regexp_substr(o.value, 'to.*\\+[0-9]{11}'), -11)
                else null 
                end as to_number
            ,substr(o.value, regexp_instr(o.value, 'Message: ', 1, 1)+9) as message
        from
            src_hs_object_properties o
            join allcomm a on o.objectid = a.objectid
        where
            name in('hs_communication_body', 'hs_call_body')
            -- and o.objecttypeid <> '0-4'
    )

    ,obj as(
        select
            o.value as ownerid
            ,r.*
        from
            rawdata r
            left join src_hs_object_properties o 
                on o.objectid = r.objectid
                and o.name = 'hubspot_owner_id'
    )

    ,tmstmp as(
        select
            o.objectid
            ,to_timestamp(o.value) as timestamp
        from
            rawdata r
            left join src_hs_object_properties o 
                on o.objectid = r.objectid
                and o.name = 'hs_timestamp'
    )

    ,hs as(
        select distinct
            o.firstname
            ,o.lastname
            ,r.contact_method
            ,case
                when r.message_direction = 'Outgoing' then 'Outbound'
                when r.message_direction = 'Incoming' then 'Inbound'
                else r.message_direction
                end as message_direction
            ,r.from_number
            ,r.to_number
            ,r.message
            ,t.timestamp
        from
            obj r
            left join src_hs_owners o on r.ownerid = nullif(trim(o.ownerid), '')
            left join tmstmp t on r.objectid = t.objectid
    )

    ,final as(
        select
            nvl(l.lead_pk, (select lead_pk from dim_lead where lead_id = '0')) as lead_pk
            ,sf.contact_method
            ,sf.created_date
            ,sf.caller_id
            ,sf.phone
            ,sf.direction
            ,sf.call_duration_in_seconds
            ,concat(lpad(nvl(hours, '0'),2,0), ':', lpad(nvl(minutes, '0'),2,0), ':', lpad(nvl(seconds, '0'),2,0)) as call_duration
            ,sf.call_twilio_client as user
            ,sf.activity_name
            ,'SF' as source
            ,null as message
            ,null as message_id
            ,null as response_time
        from 
            sf
            left join dim_lead l on sf.lead_id = l.lead_id

        union
        select
            (select lead_pk from dim_lead where lead_id = '0') as lead_pk
            ,contact_method
            ,timestamp as created_date
            ,case
                when message_direction = 'Outbound' then to_number
                when message_direction = 'Inbound' then from_number
                else null
                end as caller_id
            ,case
                when message_direction = 'Inbound' then to_number
                when message_direction = 'Outbound' then from_number
                else null
                end as phone
            ,message_direction as direction
            ,null as call_duration_in_seconds
            ,null as call_duration
            ,concat(firstname, ' ', lastname)
            ,null as activity_name
            ,'HS' as source
            ,message
            ,null as message_id
            ,null as response_time
        from
            hs

        union
        select
            (select lead_pk from dim_lead where lead_id = '0') as lead_pk
            ,'Text' as contact_method
            ,a.created_date
            ,case
                when lower(direction) like '%outbound%' then to_number
                when lower(direction) like '%inbound%' then from_number
                else null
                end as caller_id
            ,case
                when lower(direction) like '%inbound%' then to_number
                when lower(direction) like '%outbound%' then from_number
                else null
                end as phone
            ,case
                when lower(direction) like '%inbound%' then 'Inbound'
                when lower(direction) like '%outbound%' then 'Outbound'
                else null
                end as direction
            ,null as call_duration_in_seconds
            ,null as call_duration
            ,u.name
            ,null as activity_name
            ,'SF' as source
            ,a.text_body as message
            ,a.message_id
            ,a.response_time
        from 
            src_sf_twilio_sf_message_c a
            join src_sf_user u on a.owner_id = u.user_id
    )

select * from final
