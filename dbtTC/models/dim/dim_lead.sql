with
    src_sf_lead as(
        select *
        from {{ ref('src_sf_lead')}}
    )

    ,src_sf_user as(
        select *
        from {{ ref('src_sf_user')}}
    )

    ,src_sf_partner_lead_c as(
        select *
        from {{ ref('src_sf_partner_lead_c')}}
    )

    ,src_sf_account as(
        select *
        from {{ ref('src_sf_account')}}
    )

    ,final as(
        select
            l.first_name
            ,l.last_name
            ,l.name
            ,l.company
            ,l.street
            ,l.city
            ,case
                when
                    l.state is null
                    and(
                        lower(l.street) like '%dallas%'
                        or lower(l.street) like '%amarillo%'
                        or lower(l.street) like '%austin%'
                        or lower(l.street) like '%houston%'
                        or lower(l.street) like '%leander%'
                        or lower(l.street) like '%belton%'
                        or lower(l.street) like '%lufkin%'
                        or lower(l.street) like '%tx%'
                        or lower(l.street) like '%texas%'
                        or lower(l.street) like '%prosper%'
                        or lower(l.street) like '%kilgore%'
                    )
                then 'TX'
                when lower(l.state) like '%texas%' then 'TX'
                when l.state is null and(lower(l.street) like '%tn%') then 'TN'
                when l.state is null and(lower(l.street) like '% az %') then 'AZ'
                when l.state is null and(lower(l.street) like '% or %') then 'OR'
                when l.state is null and(lower(l.street) like '% ca %') then 'CA'
                when l.state is null and(lower(l.street) like '% fl %') then 'FL'
                when l.state is null and(lower(l.street) like '%mn%') then 'MN'
                when l.state is null and(lower(l.street) like '% al %') then 'AL'
                when l.state is null and(lower(l.street) like '% co %') then 'CO'
                when l.state is null and(lower(l.street) like '% mi %') then 'MI'
                when l.state is null and(lower(l.street) like '% wa %') then 'WA'
                when l.state is null and(lower(l.street) like '% il %') then 'IL'
                when l.state is null and(lower(l.street) like '% ar %') then 'AR'
                when l.state is null and(lower(l.street) like '% nc %') then 'NC'
                when l.state is null and(lower(l.street) like '% sc %') then 'SC'
                when l.state is null and(lower(l.street) like '% mo %') then 'MO'
                when l.state is null and(lower(l.street) like '% nj %') then 'NJ'
                when l.state is null and(lower(l.street) like '% pa %') then 'PA'
                when l.state is null and(lower(l.street) like '% ms %') then 'MS'
                when l.state is null and(lower(l.street) like '% ky %') then 'KY'
                when l.state is null and(lower(l.street) like '% id%') then 'ID'
                when l.state is null and(lower(l.street) like '% wi %') then 'WI'
                when l.state is null and(lower(l.street) like '% va %') then 'VA'
                when l.state is null and(lower(l.street) like '% ga %') then 'GA'
                when l.state is null and(lower(l.street) like '% ks %') then 'KS'
                when l.state is null and(lower(l.street) like '% nv %') then 'NV'
                when l.state is null and(lower(l.street) like '% me %') then 'ME'
                when l.state is null and(lower(l.street) like '% oh %') then 'OH'
                when l.state is null and(lower(l.street) like '% in %') then 'IN'
                when l.state is null and(lower(l.street) like '%mt%') then 'MT'
                when l.state is null and(lower(l.street) like '%illinois%') then 'IL'
                else l.state
                end as state
            ,l.postal_code
            ,l.country
            ,concat(
                l.street
                ,case when l.city is not null then ', ' || l.city else '' end
                ,case when l.state is not null then ', ' || l.state else '' end
                ,case when l.postal_code is not null then ', ' || l.postal_code else '' end
                ,case when l.country is not null then ', ' || l.country else '' end
            ) as full_address
            ,l.mobile_phone
            ,l.email
            ,l.lead_source
            ,a.account_name as partner_name
            ,l.created_date
            ,u.name as owner_name

        from
            src_sf_lead l
            join src_sf_user u on l.owner_id = u.id
            left join src_sf_partner_lead_c c on l.lead_id = c.lead_c
            left join src_sf_account a on c.partner_c = a.account_id

        where
            l.is_deleted = 'FALSE'
    )

select * from final