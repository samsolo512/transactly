with
    src_sf_lead as(
        select *
        from {{ ref('src_sf_lead') }}
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
    ,recent_partner as(
        select
            lead_c as lead_id
            ,max(created_date) as created_date
        from src_sf_partner_lead_c
        group by lead_c
    )  -- select * from FIVETRAN.SALESFORCE.PARTNER_LEAD_C where lead_c = '00Q5w0000236cCtEAI'

    ,partner as(
        select
            c.lead_c as lead_id
            ,min(a.account_name) as partner
        from
            src_sf_partner_lead_c c
            join recent_partner p
                on c.lead_c = p.lead_id
                and c.created_date = p.created_date
            left join src_sf_account a on c.partner_c = a.account_id
        group by c.lead_c
    )

    ,final as(
        select
            working.seq_dim_lead.nextval as lead_pk
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
            ,l.email
            ,l.lead_source
            ,l.created_date as lead_created_date
            ,l.agent_name
            ,l.agent_email
            ,p.partner

        from
            lead_id ul
            join src_sf_lead l
                on ul.email = l.email
                and ul.lead_id = l.lead_id
            left join states on l.lead_id = states.lead_id
            left join partner p on l.lead_id = p.lead_id
    )

select * from final

-- select lead_id, count(1) from final group by lead_id order by count(1) desc





/*
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

    ,src_sf_contact as(
        select *
        from {{ ref('src_sf_contact')}}
    )

    ,src_sf_opportunity as(
        select *
        from {{ ref('src_sf_opportunity')}}
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

    ,contact as(
        select distinct
            c.agent_c
            ,c.agent_brokerage_c
            ,c.street
            ,c.zip
            ,c.email
            ,c.last_name
            ,c.first_name
            ,c.full_name
            ,c.phone
            ,c.owner_id
            ,c.created_date as contact_created_date
            ,c.converted_lead_c

            ,ac.account_name as contact_account_name
            ,a.account_name as opportunity_account_name

            ,o.close_date as opportunity_close_date
            ,o.created_date_time
            ,o.created_date as opportunity_created_date
            ,o.opportunity_name
            ,o.owner_id
            ,o.stage

        from
            src_sf_contact c
            left join src_sf_account ac on c.account_id = ac.account_id
            left join src_sf_opportunity o on c.contact_id = o.contact_id
            left join src_sf_account a on o.account_id = a.account_id
            left join src_sf_user u on o.owner_id = u.user_id
    )

    ,max_close as(
        select
            c.email
            ,c.street
            ,c.opportunity_name
            ,max(c.opportunity_close_date) as opportunity_close_date

        from
            contact c

        group by c.email, c.street, c.opportunity_name
    )

    ,max_created as(
        select
            c.email
            ,c.street
            ,c.opportunity_name
            ,c.opportunity_close_date
            ,max(created_date_time) as created_date_time
        from
            contact c
            join max_close mc
                on c.email = mc.email
                and c.street = mc.street
                and c.opportunity_name = mc.opportunity_name
                and c.opportunity_close_date = mc.opportunity_close_date
        group by c.email, c.street, c.opportunity_name, c.opportunity_close_date
    )

    ,lead_max as(
        select
            l.email
            ,max(l.created_date_time) as created_date_time

        from
            src_sf_lead l
            left join contact cont
                on l.email = cont.email
                and jarowinkler_similarity(l.street, cont.street) >= 80

        group by l.email
    )

    ,final as(
        select
            working.seq_dim_lead.nextval as lead_pk
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
            ,l.email
            ,l.lead_source
            ,l.created_date as lead_created_date
            ,l.agent_name
            ,l.agent_email

            ,u.name as owner_name
            ,c.lead_account_name as lead_partner_name
            ,cont.contact_account_name as contact_partner_name
            ,cont.opportunity_account_name as opportunity_partner_name
            ,cont.contact_created_date
            ,cont.opportunity_created_date
            ,cont.opportunity_close_date
            ,cont.opportunity_name
            ,cont.stage

        from
            src_sf_lead l
            join lead_max lm
                on l.email = lm.email
                and l.created_date_time = lm.created_date_time
            join src_sf_user u on l.owner_id = u.user_id
            left join(
                select distinct
                    c.lead_c
                    ,a.account_name as lead_account_name
                from
                    src_sf_partner_lead_c c
                    left join src_sf_account a on c.partner_c = a.account_id
            ) c on l.lead_id = c.lead_c
            left join contact cont
                join max_created mc
                    on cont.email = mc.email
                    and cont.street = mc.street
                    and cont.opportunity_name = mc.opportunity_name
                    and cont.opportunity_close_date = mc.opportunity_close_date
                    and cont.created_date_time = mc.created_date_time
                on l.lead_id = cont.converted_lead_c
                -- on l.email = cont.email
                -- and jarowinkler_similarity(l.street, cont.street) >= 80
            left join states on l.lead_id = states.lead_id

    )

select * from final
*/