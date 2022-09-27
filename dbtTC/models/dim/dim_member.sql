with
    src_tc_member as(
        select *
        from {{ ref('src_tc_member') }}
    )

    ,src_tc_transaction_role as(
        select *
        from {{ ref('src_tc_transaction_role') }}
    )

    ,src_tc_party as(
        select *
        from {{ ref('src_tc_party') }}
    )

    ,dim_user as(
        select *
        from {{ ref('dim_user') }}
    )

    ,src_tc_office as(
        select *
        from {{ ref('src_tc_office') }}
    )

    ,final as(
        select
            working.seq_dim_member.nextval as member_pk
            ,m.member_id
            ,tr.role_name
            ,p.party_name as side_id
            ,case
                when m.is_active = 'TRUE' then 1
                when m.is_active = 'FALSE' then 0
                else null
                end as active_flag
            ,u.user_id
            ,u.first_name
            ,u.last_name
            ,u.email
            ,u.transactly_home_insurance_vendor_status
            ,u.transactly_utility_connection_vendor_status
            ,o.office_name

        from
            src_tc_member m
            left join dim_user u on m.user_id = u.user_id
            join src_tc_transaction_role tr on m.role_id = tr.role_id
            join src_tc_party p on m.party_id = p.party_id
            left join src_tc_office o on m.office_id = o.office_id

        union select 0, 0, null, null, null, null, null, null, null, null, null, null
    )

select * from final
