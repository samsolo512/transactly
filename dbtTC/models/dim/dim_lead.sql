with
    src_sf_lead as(
        select *
        from {{ ref('src_sf_lead')}}
    )

    ,src_sf_user as(
        select *
        from {{ ref('src_sf_user')}}
    )

    ,final as(
        select
            l.first_name
            ,l.last_name
            ,l.name
            ,l.company
            ,l.street
            ,l.city
            ,l.state
            ,l.postal_code
            ,l.country
            ,l.mobile_phone
            ,l.email
            ,l.lead_source
            ,l.created_date
            ,u.first_name as owner_first_name
            ,u.last_name as owner_last_name
            ,u.name as owner_name
            ,u.title as owner_title
            ,u.street as owner_street
            ,u.city as owner_city
            ,u.postal_code as owner_postal_code
            ,u.country as owner_country
            ,u.email as owner_email
            ,u.phone as owner_phone
            ,u.mobile_phone as owner_mobile_phone
            ,case when u.is_active = 'TRUE' then 1 else 0 end as owner_is_active_flag
        from
            src_sf_lead l
            join src_sf_user u on l.owner_id = u.id
        where
            l.is_deleted = 'FALSE'
    )

select * from final