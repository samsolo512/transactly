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

                        or (l.city is null and l.street is not null and a.account_name like '2TIO%')
                    )
                then 'TX'

                else l.state
                end as state

            ,l.zip
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
            ,a.account_name as partner_name
            ,l.created_date
            ,u.name as owner_name

        from
            src_sf_lead l
            join src_sf_user u on l.owner_id = u.user_id
            left join src_sf_partner_lead_c c on l.lead_id = c.lead_c
            left join src_sf_account a on c.partner_c = a.account_id

    )

select * from final