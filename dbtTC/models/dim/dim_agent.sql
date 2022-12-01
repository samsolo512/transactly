-- dim_agent

with
    src_tc_user as(
        select *
        from {{ ref('src_tc_user') }}
    )

    ,src_tc_user_role as(
        select *
        from {{ ref('src_tc_user_role') }}
    )

    ,src_tc_role as(
        select *
        from {{ ref('src_tc_role') }}
    )

    ,src_sf_lead as(
        select *
        from {{ ref('src_sf_lead') }}
    )

    -- leads
    ,lead_date as(
        select
            lower(l.agent_email) as agent_email
            ,min(l.created_date_time) as created_date_time
        from
            src_sf_lead l
        group by lower(l.agent_email)
    )

    ,lead_id as(
        select
            lower(l.agent_email) as agent_email
            ,l.created_date_time
            ,min(l.agent_name) as agent_name
        from
            src_sf_lead l
            join lead_date ld
                on lower(l.agent_email) = lower(ld.agent_email)
                and ifnull(l.created_date_time, '1/1/1900') = ifnull(ld.created_date_time, '1/1/1900')
        group by lower(l.agent_email), l.created_date_time
    )

    -- users who are agents
    ,mod_user as(
        select
            u.user_id
            ,lower(u.email) as email
            ,u.created_date
            ,u.fullname
        from
            src_tc_user u
            left join src_tc_user_role ur on u.user_id = ur.user_id
            left join src_tc_role r on ur.role_id = r.role_id
        where
            r.role = 'ROLE_AGENT'
    )

    ,combine as(
        -- users
        select
            u.email as agent_email
            ,u.fullname as agent_name
            ,u.created_date as user_created_date
            ,null as lead_created_date
            ,1 as tc_agent_flag
            ,0 as lead_agent_flag
        from
            mod_user u

        -- lead
        union
        select
            lower(l.agent_email) as agent_email
            ,l.agent_name
            ,null as user_created_date
            ,min(l.created_date) as lead_created_date
            ,0 as tc_agent_flag
            ,1 as lead_agent_flag
        from
            src_sf_lead l
            join lead_id ld
                on lower(l.agent_email) = lower(ld.agent_email)
                and ifnull(l.created_date_time, '1/1/1900') = ifnull(ld.created_date_time, '1/1/1900')
                and ifnull(l.agent_name, '-1') = ifnull(ld.agent_name, '-1')
        group by
            lower(l.agent_email), l.agent_name
    )

    ,client_flags as(
        select
            agent_email
            ,min(agent_name) as agent_name
            ,min(user_created_date) as user_created_date
            ,min(lead_created_date) as lead_created_date
            ,max(tc_agent_flag) as tc_agent_flag
            ,max(lead_agent_flag) as lead_agent_flag
        from
            combine
        group by agent_email
    )

    ,final as(
        select
            working.seq_dim_agent.nextval as agent_pk

            -- grain
            ,nvl(u.email, c.agent_email) as agent_email

            ,u.user_id
            ,nvl(u.fullname, c.agent_name) as agent_name
            ,c.user_created_date as tc_created_date
            ,c.lead_created_date as lead_created_date
            ,c.tc_agent_flag
            ,c.lead_agent_flag
            ,case
                when c.tc_agent_flag = 1 and c.lead_agent_flag = 0 then 'TC'
                when c.tc_agent_flag = 0 and c.lead_agent_flag = 1 then 'Lead'
                when c.tc_agent_flag = 1 and c.lead_agent_flag = 1 then 'TC and Lead'
                else null
                end as agent_type
        from
            client_flags c
            left join mod_user u on c.agent_email = u.email
    )

select * from final

-- select agent_email, count(1) from final group by agent_email order by count(1) desc