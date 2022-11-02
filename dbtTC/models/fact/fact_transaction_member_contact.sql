with
    src_tc_transaction_transactly_vendor as(
        select *
        from {{ ref('src_tc_transaction_transactly_vendor') }}
    )

    ,src_tc_transactly_vendor as(
        select *
        from {{ ref('src_tc_transactly_vendor') }}
    )

    ,src_tc_transaction as(
        select *
        from {{ ref('src_tc_transaction') }}
    )

    ,dim_transaction as(
        select *
        from {{ ref('dim_transaction') }}
    )

    ,src_tc_member as(
        select *
        from {{ ref('src_tc_member') }}
    )

    ,dim_member_contact as(
        select *
        from {{ ref('dim_member_contact') }}
    )

    ,src_tc_contact as(
        select *
        from {{ ref('src_tc_contact') }}
    )

    ,src_tc_user as(
        select *
        from {{ ref('src_tc_user') }}
    )

    ,src_tc_user_transactly_vendor_opt_out as(
        select *
        from {{ ref('src_tc_user_transactly_vendor_opt_out') }}
    )

    -- transfer utilities
    ,ttv_utility as(
        select
            ttv.transaction_id,
            ttv.notified_date,
            tv.internal_name
        from
            src_tc_transaction_transactly_vendor ttv
            join src_tc_transactly_vendor tv
                on ttv.transactly_vendor_id = tv.vendor_id
                and tv.vendor_type_id = 10  -- utility transfer
    )

    -- home insurance
    ,ttv_home_insurance as(
        select
            ttv.transaction_id,
            ttv.notified_date,
            tv.internal_name
        from
            src_tc_transaction_transactly_vendor ttv
            join src_tc_transactly_vendor tv
                on ttv.transactly_vendor_id = tv.vendor_id
                and tv.vendor_type_id = 7  -- home insurance
    )

    -- combine members and contacts
--     ,member_contact as(
--         -- member
--         select
--             trans.transaction_id
--             ,lower(memb.email) as email
--             ,m.member_id as member_contact_id
--             ,memb.first_name
--             ,memb.last_name
--             ,memb.role_name as role
--             ,1 as member_flag
--             ,0 as contact_flag
--
--         from
--             src_tc_transaction trans
--             join dim_transaction dt on trans.transaction_id = dt.transaction_id
--             join src_tc_member m on trans.transaction_id = m.transaction_id
--             left join dim_member memb on m.member_id = memb.member_id
--
--         -- contact
--         union all
--         select
--             trans.transaction_id
--             ,lower(cont.email) as email
--             ,cont.contact_id as member_contact_id
--             ,cont.first_name
--             ,cont.last_name
--             ,c.role_name as role
--             ,0 as member_flag
--             ,1 as contact_flag
--
--         from
--             src_tc_transaction trans
--             join dim_transaction dt on trans.transaction_id = dt.transaction_id
--             join src_tc_contact cont on trans.transaction_id = cont.transaction_id
--             left join dim_contact c on cont.contact_id = c.contact_id
--     )

    -- all members and contacts
--     ,combine as(
--         select
--             transaction_id
--             ,lower(email) as email
--             ,max(member_id) as member_id
--             ,max(contact_id) as contact_id
--             ,first_name
--             ,last_name
--             ,member_role as member_role
--             ,contact_role as contact_role
--             ,max(member_flag) as member_flag
--             ,max(contact_flag) as contact_flag
--
--         from
--             member_contact mc
--
--         group by
--             transaction_id, lower(email), first_name, last_name, member_role, contact_role
--     )

    ,final as(
        select
            t.transaction_pk
            ,mc.member_contact_pk

            -- utility and insurance info
            ,ttv_utility.internal_name as utility_lead_sent_to
            ,DATE(ttv_utility.notified_date) as utility_notified_date
            ,ttv_home_insurance.internal_name as home_insurance_lead_sent_to
            ,DATE(ttv_home_insurance.notified_date) as home_insurance_notified_date

            ,CASE
                WHEN (ttv_utility.internal_name is not null) THEN 'LEAD SENT'

                WHEN
                    t.transaction_id in (
                        select transaction_id
                        from
                            src_tc_member member
                            join src_tc_user usr on member.user_id = usr.user_id
                            join src_tc_user_transactly_vendor_opt_out
                                on member.user_id = src_tc_user_transactly_vendor_opt_out.user_id
                        where
                            member.role_id = 7
                            and src_tc_user_transactly_vendor_opt_out.vendor_type_id = 10
                    )
                THEN 'AGENT OPTED OUT'

                WHEN
                    t.transaction_id not in (
                        select transaction_id
                        from src_tc_contact contact
                        where contact.role_id = 6
                    )
                    and t.transaction_id not in (
                        select transaction_id
                        from src_tc_member member
                        where member.role_id = 6
                    )
                THEN 'NO BUYER'

                WHEN
                    t.transaction_id not in (
                        select transaction_id
                        from src_tc_contact contact
                        where
                            contact.role_id = 6
                            and COALESCE(contact.email, '') != ''
                            and COALESCE(contact.phone, '') != '')
                            and t.transaction_id not in (
                                select transaction_id
                                from
                                    src_tc_member member
                                    join src_tc_user usr on member.user_id = usr.user_id
                                where
                                    member.role_id = 6
                                    and COALESCE(usr.email, '') != '' and COALESCE(usr.phone, '') != ''
                            )
                THEN 'INCOMPLETE BUYER'

                ELSE 'READY FOR SENDING'
                END as utility_transfer_status

            ,CASE
                WHEN ttv_home_insurance.internal_name is not null THEN 'LEAD SENT'
                WHEN
                    t.transaction_id in (
                        select transaction_id
                        from
                            src_tc_member member
                            join src_tc_user usr on member.user_id = usr.user_id
                            join src_tc_user_transactly_vendor_opt_out
                                on member.user_id = src_tc_user_transactly_vendor_opt_out.user_id
                        where
                            member.role_id = 7
                            and src_tc_user_transactly_vendor_opt_out.vendor_type_id = 7
                    )
                THEN 'AGENT OPTED OUT'

                WHEN
                    t.transaction_id not in (
                        select transaction_id
                        from src_tc_contact contact
                        where contact.role_id = 6
                    )
                    and t.transaction_id not in (
                        select transaction_id
                        from src_tc_member member
                        where member.role_id = 6
                    )
                THEN 'NO BUYER'

                WHEN
                    t.transaction_id not in (
                        select transaction_id
                        from src_tc_contact contact
                        where
                            contact.role_id = 6
                            and COALESCE(contact.email, '') != ''
                            and COALESCE(contact.phone, '') != '')
                            and t.transaction_id not in (
                                select transaction_id
                                from
                                    src_tc_member member
                                    join src_tc_user user on member.user_id = user.user_id
                                    where
                                        member.role_id = 6
                                        and COALESCE(user.email, '') != ''
                                        and COALESCE(user.phone, '') != ''
                            )
                THEN 'INCOMPLETE BUYER'

                ELSE 'READY FOR SENDING'
                END as home_insurance_status

        from
            src_tc_transaction trans
            join dim_transaction t on trans.transaction_id = t.transaction_id
            left join dim_member_contact mc on t.transaction_id = mc.transaction_id
            left join ttv_utility on ttv_utility.transaction_id = t.transaction_id
            left join ttv_home_insurance on ttv_home_insurance.transaction_id = t.transaction_id
    )

select * from final
-- the following is for Alissa's report
-- where
--     (contact_role = 'Buyer' or member_role = 'Buyer')
--     and status not in('cancelled', 'withdrawn')
--     and order_side = 'buyer'
--     and closing_date is not null
