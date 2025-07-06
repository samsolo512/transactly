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

    -- combined util and insurance case statements
    ,util_insurance as(
        select
            t.transaction_id
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
                            member.role_id = 7  -- buyer agent
                            and src_tc_user_transactly_vendor_opt_out.vendor_type_id = 10  -- utility transfer
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
                            -- and COALESCE(contact.phone, '') != ''
                    )
                    and t.transaction_id not in (
                        select transaction_id
                        from
                            src_tc_member member
                            join src_tc_user usr on member.user_id = usr.user_id
                        where
                            member.role_id = 6
                            and COALESCE(usr.email, '') != '' 
                            -- and COALESCE(usr.phone, '') != ''
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
            dim_transaction t
            left join ttv_utility on ttv_utility.transaction_id = t.transaction_id
            left join ttv_home_insurance on ttv_home_insurance.transaction_id = t.transaction_id
    )

    ,final as(
        select
            t.transaction_pk
            ,mc.member_contact_pk

            -- utility and insurance info
            ,u.internal_name as utility_lead_sent_to
            ,date(u.notified_date) as utility_notified_date
            ,i.internal_name as home_insurance_lead_sent_to
            ,date(i.notified_date) as home_insurance_notified_date
            ,ui.utility_transfer_status
            ,ui.home_insurance_status
            ,o.status_changed_date

            -- for testing in the 'where' clause below
            -- ,mc.role_name as member_contact_role
            -- ,t.status as transfer_status
            -- ,t.transaction_side
            -- ,t.contract_closing_date as closing_date

        from
            src_tc_transaction trans
            join dim_transaction t on trans.transaction_id = t.transaction_id
            left join dim_member_contact mc on t.transaction_id = mc.transaction_id
            left join ttv_utility u on u.transaction_id = t.transaction_id
            left join ttv_home_insurance i on i.transaction_id = t.transaction_id
            left join util_insurance ui on trans.transaction_id = ui.transaction_id
            left join dim_order o on trans.transaction_id = o.transaction_id
    )

select * from final
-- the following is for Alissa's report
-- where
--     member_contact_role = 'Buyer'
--     and transfer_status not in('cancelled', 'withdrawn')
--     and transaction_side = 'buyer'
--     and closing_date is not null
