with
    src_sf_lead as(
        select *
        from {{ ref('src_sf_lead') }}
    )

    ,src_sf_account as(
        select *
        from {{ ref('src_sf_account') }}
    )

    ,src_sf_user as(
        select *
        from {{ ref('src_sf_user') }}
    )

    ,src_sf_contact as(
        select *
        from {{ ref('src_sf_contact') }}
    )

    ,src_sf_partner_lead_c as(
        select *
        from {{ ref('src_sf_partner_lead_c') }}
    )

    ,states as(
        select
            lead_id
            ,case
                when l.state is null and(lower(l.city) like '%atlanta%') then 'GA'
                when l.state is null and(lower(l.city) like '%phoenix%') then 'AZ'
                when l.state is null and(lower(l.city) like '%las vegas%') then 'NV'
                when l.state is null and(lower(l.city) like '%san antonio%') then 'TX'
                when l.state is null and(lower(l.city) like '%charlotte%') then 'NC'
                when l.state is null and(lower(l.city) like '%chicago%') then 'IL'
                when l.state is null and(lower(l.city) like '%denver%') then 'CO'
                when l.state is null and(lower(l.city) like '%los angeles%') then 'CA'
                when l.state is null and(lower(l.city) like '%washington%') then 'BC'
                when l.state is null and(lower(l.city) like '%tampa%') then 'FL'
                when l.state is null and(lower(l.city) like '%orlando%') then 'FL'
                when l.state is null and(lower(l.city) like '%miami%') then 'FL'
                when l.state is null and(lower(l.city) like '%jacksonville%') then 'FL'
                when l.state is null and(lower(l.city) like '%nashville%') then 'TN'
                when l.state is null and(lower(l.city) like '%memphis%') then 'TN'
                when l.state is null and(lower(l.city) like '%san diego%') then 'CA'
                when l.state is null and(lower(l.city) like '%fresno%') then 'CA'
                when l.state is null and(lower(l.city) like '%san francisco%') then 'CA'
                when l.state is null and(lower(l.city) like '%kansas city%') then 'MO'
                when l.state is null and(lower(l.city) like '%philadelphia%') then 'PA'
                when l.state is null and(lower(l.city) like '%pittsburgh%') then 'PA'
                when l.state is null and(lower(l.city) like '%new york%') then 'NY'
                when l.state is null and(lower(l.city) like '%portland%') then 'OR'
                when l.state is null and(lower(l.city) like '%fort worth%') then 'TX'
                when l.state is null and(lower(l.city) like '%seattle%') then 'WA'
                when l.state is null and(lower(l.city) like '%sacramento%') then 'CA'
                when l.state is null and(lower(l.city) like '%detroit%') then 'MI'
                when l.state is null and(lower(l.city) like '%minneapolis%') then 'MN'
                when l.state is null and(lower(l.city) like '%indianapolis%') then 'IN'
                when l.state is null and(lower(l.city) like '%columbus%') then 'OH'
                when l.state is null and(lower(l.city) like '%cincinnati%') then 'OH'
                when l.state is null and(lower(l.city) like '%st. louis%') then 'MO'
                when l.state is null and(lower(l.city) like '%baltimore%') then 'MD'
                when l.state is null and(lower(l.city) like '%baltimore%') then 'AZ'
                when l.state is null and(lower(l.city) like '%albuquerque%') then 'NM'
                when l.state is null and(lower(l.city) like '%tucson%') then 'AZ'
                when l.state is null and(lower(l.city) like '%el paso%') then 'NM'
                when l.state is null and(lower(l.city) like '%riverside%') then 'CA'
                when l.state is null and(lower(l.city) like '%oklahoma city%') then 'OK'
                when l.state is null and(lower(l.city) like '%boston%') then 'MA'
                when l.state is null and(lower(l.city) like '%arlington%') then 'VA'
                when l.state is null and(lower(l.city) like '%roanoke%') then 'VA'
                when l.state is null and(lower(l.city) like '%bedford%') then 'CT'
                when l.state is null and(lower(l.city) like '%plano%') then 'TX'

                when l.state is null and(lower(l.street) like '% al %') then 'AL'
                when l.state is null and(lower(l.street) like '% ar %') then 'AR'
                when l.state is null and(lower(l.street) like '% az %') then 'AZ'
                when l.state is null and(lower(l.street) like '% ca %') then 'CA'
                when l.state is null and(lower(l.street) like '% co %') then 'CO'
                when l.state is null and(lower(l.street) like '%colorado%') then 'CO'
                when l.state is null and(lower(l.street) like '% de %') then 'DE'
                when l.state is null and(lower(l.street) like '% fl %') then 'FL'
                when l.state is null and(lower(l.street) like '% ga %') then 'GA'
                when l.state is null and(lower(l.street) like '% ga') then 'GA'
                when l.state is null and(lower(l.street) like '% hi %') then 'HI'
                when l.state is null and(lower(l.street) like '% id%') then 'ID'
                when l.state is null and(lower(l.street) like '% il %') then 'IL'
                when l.state is null and(lower(l.street) like '% il.%') then 'IL'
                when l.state is null and(lower(l.street) like '%illinois%') then 'IL'
                when l.state is null and(lower(l.street) like '% in %') then 'IN'
                when l.state is null and(lower(l.street) like '% ks %') then 'KS'
                when l.state is null and(lower(l.street) like '% ky %') then 'KY'
                when l.state is null and(lower(l.street) like '% la %') then 'LA'
                when l.state is null and(lower(l.street) like '% ma %') then 'MA'
                when l.state is null and(lower(l.street) like '% md %') then 'MD'
                when l.state is null and(lower(l.street) like '% me %') then 'ME'
                when l.state is null and(lower(l.street) like '% mi %') then 'MI'
                when l.state is null and(lower(l.street) like '% mo %') then 'MO'
                when l.state is null and(lower(l.street) like '%mn%') then 'MN'
                when l.state is null and(lower(l.street) like '% ms %') then 'MS'
                when l.state is null and(lower(l.street) like '%mt%') then 'MT'
                when l.state is null and(lower(l.street) like '% nc %') then 'NC'
                when l.state is null and(lower(l.street) like '% nh %') then 'NH'
                when l.state is null and(lower(l.street) like '% nj %') then 'NJ'
                when l.state is null and(lower(l.street) like '% nv %') then 'NV'
                when l.state is null and(lower(l.street) like '% ny %') then 'NY'
                when l.state is null and(lower(l.street) like '% oh %') then 'OH'
                when l.state is null and(lower(l.street) like '% ok %') then 'OK'
                when l.state is null and(lower(l.street) like '% ok') then 'OK'
                when l.state is null and(lower(l.street) like '% or %') then 'OR'
                when l.state is null and(lower(l.street) like '% or') then 'OR'
                when l.state is null and(lower(l.street) like '% pa %') then 'PA'
                when l.state is null and(lower(l.street) like '% sc %') then 'SC'
                when l.state is null and(lower(l.street) like '% sd %') then 'SD'
                when l.state is null and(lower(l.street) like '%tn%') then 'TN'
                when l.state is null and(lower(l.street) like '% va %') then 'VA'
                when l.state is null and(lower(l.street) like '% vt %') then 'VT'
                when l.state is null and(lower(l.street) like '% wa %') then 'WA'
                when l.state is null and(lower(l.street) like '% wi %') then 'WI'

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

                        or lower(l.city) like '%dallas%'
                        or lower(l.city) like '%amarillo%'
                        or lower(l.city) like '%austin%'
                        or lower(l.city) like '%houston%'
                        or lower(l.city) like '%leander%'
                        or lower(l.city) like '%belton%'
                        or lower(l.city) like '%lufkin%'
                        or lower(l.city) like '%tx%'
                        or lower(l.city) like '%texas%'
                        or lower(l.city) like '%prosper%'
                        or lower(l.city) like '%kilgore%'

                        -- or (l.city is null and l.street is not null and c.lead_account_name like '2TIO%')
                    )
                then 'TX'

                when lower(l.state) = 'texas' then 'TX'
                when lower(l.state) = 'tx' then 'TX'
                when lower(l.state) = 'tx 75495' then 'TX'
                when lower(l.state) = 'north carolina' then 'NC'
                when lower(l.state) = 'arizona' then 'AZ'
                when lower(l.state) = 'georgia' then 'GA'
                when lower(l.state) = 'massachusetts' then 'MA'
                when lower(l.state) = 'colorado' then 'CO'
                when lower(l.state) = 'co' then 'CO'
                when lower(l.state) = 'alabama' then 'AL'
                when lower(l.state) = 'california' then 'CA'
                when lower(l.state) = 'connecticut' then 'CT'
                when lower(l.state) = 'louisiana' then 'LA'
                when lower(l.state) = 'arkansas' then 'AR'
                when lower(l.state) = 'florida' then 'FL'
                when lower(l.state) = 'new york' then 'NY'

                else l.state
                end as state

        from src_sf_lead l
    )

    -- lead
    ,lead_date as(
        select
            l.email
            ,max(l.created_date_time) as created_date_time
        from
            src_sf_lead l
        group by l.email
    )

    ,lead_id as(
        select
            l.email
            ,min(l.lead_id) as lead_id
        from
            src_sf_lead l
            join lead_date ld
                on l.email = ld.email
                and l.created_date_time = ld.created_date_time
        group by l.email
    )

    -- partner
--     ,recent_partner as(
--         select
--             lead_c as lead_id
--             ,max(created_date) as created_date
--         from src_sf_partner_lead_c
--         group by lead_c
--     )  -- select * from FIVETRAN.SALESFORCE.PARTNER_LEAD_C where lead_c = '00Q5w0000236cCtEAI'
--
--     ,partner as(
--         select
--             c.lead_c as lead_id
--             ,min(a.account_name) as partner
--         from
--             src_sf_partner_lead_c c
--             join recent_partner p
--                 on c.lead_c = p.lead_id
--                 and c.created_date = p.created_date
--             left join src_sf_account a on c.partner_c = a.account_id
--         group by c.lead_c
--     )
--
--     ,partner_acct as(
--         select
--             p.lead_id
--             ,p.partner
--             ,u.name as partner_account_name
--         from
--             src_sf_account a
--             join partner p on a.account_name = p.partner
--             left join src_sf_user u on a.owner_id = u.user_id
--     )

    ,contact as(
        select
            lead_id
            ,max(created_date_time) as created_date_time
        from src_sf_contact c
        group by lead_id
    )

    -- get 1 acct per lead
    ,max_acct as(
        select
            c.lead_c
            ,max(c.created_date) as created_date
        from
            src_sf_account a
            left join src_sf_partner_lead_c c on c.partner_c = a.account_id
            -- left join src_sf_lead l on c.lead_c = l.lead_id
        --where
            --account_name not like '%Lead Abandon%'
        group by c.lead_c
    )

    ,acct as(
        select
            l.lead_id
            ,c.created_date
            ,min(a.account_id) as account_id
        from
            src_sf_account a
            left join src_sf_partner_lead_c c on c.partner_c = a.account_id
            left join src_sf_lead l on c.lead_c = l.lead_id
            join max_acct ma
                on c.lead_c = ma.lead_c
                and c.created_date = ma.created_date
        --where
            --account_name not like '%Lead Abandon%'
        group by
            l.lead_id, c.created_date
    )

    ,unique_lead as(
        select
            a.account_name
            ,u.name as account_owner
            ,l.name as lead_name
            ,l.lead_id
            ,c.created_date
            ,a.parent_id
        from
            src_sf_account a
            left join src_sf_user u on a.owner_id = u.user_id
            left join src_sf_partner_lead_c c on c.partner_c = a.account_id
            left join src_sf_lead l on c.lead_c = l.lead_id
            join acct ma
                on l.lead_id = ma.lead_id
                and c.created_date = ma.created_date
                and a.account_id = ma.account_id
        --where
            --account_name not like '%Lead Abandon%'
    )

    ,final as(
        select
            working.seq_dim_lead.nextval as lead_pk

            -- grain
            ,l.lead_id

            ,l.first_name
            ,l.last_name
            ,l.name
            ,l.company
            ,l.street
            ,l.city
            ,states.state
            ,case
                when l.zip is null then regexp_substr(l.street, '[0-9]{5}', 2)
                else l.zip
                end as zip
            ,l.country
            ,concat(
                l.street
                ,case when l.city is not null then ', ' || l.city else '' end
                ,case when l.state is not null then ', ' || l.state else '' end
                ,case when l.zip is not null then ', ' || l.zip else '' end
                ,case when l.country is not null then ', ' || l.country else '' end
            ) as full_address
            ,l.phone
            ,l.mobile_phone
            ,l.email
            ,l.lead_source
            ,l.created_date as lead_created_date
            ,l.agent_name
            ,l.agent_email
--             ,p.partner
            ,c.electricity
            ,c.sewer
            ,c.trash
            ,c.water
            ,c.gas
            ,c.internet
            ,ulead.account_name
            ,ulead.account_owner
            ,b.account_name as parent_account_name
            ,u.name as owner_name
            ,last_day(l.created_date, 'week') lead_week_date
            ,l.status
            ,l.attribution

        from
            lead_id ul
            join src_sf_lead l
                on ul.email = l.email
                and ul.lead_id = l.lead_id
            left join states on l.lead_id = states.lead_id
--             left join partner p on l.lead_id = p.lead_id
            left join src_sf_contact c
                join contact
                    on c.lead_id = contact.lead_id
                    and c.created_date_time = contact.created_date_time
                on l.lead_id = c.lead_id
            left join unique_lead ulead on ul.lead_id = ulead.lead_id
            left join src_sf_account b on ulead.parent_id = b.account_id
            left join src_sf_user u on l.owner_id = u.user_id

        union
        select
            0, '0', 
            null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null, null, null, null, null, null, null, null
    )

select * from final

-- select lead_id, count(1) from final group by lead_id order by count(1) desc
