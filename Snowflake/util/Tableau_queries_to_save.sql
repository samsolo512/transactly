-- outstanding receivables
SELECT
    line_item.status,
    concat(
    tc_order.address_line_1, ' ', tc_order.city,
    ', ', tc_order.state, ' ', tc_order.zip
    ) AS 'Transaction Address',
    tc_order.state,
    line_item.agent_pays,
    line_item.office_pays,
    line_item.created AS 'Order Created Date',
    line_item.due_date,
    transaction.closed_date,
    line_item.description,
    if(line_item.tc_paid = 0,'N','Y') as TCPaid,
    if(tc_order.side_id=1,'Buyer','Seller') as OrderSide,
    line_item.cancelled_date as Cancelled_Date,
    concat(u2.first_name,' ',u2.last_name) as Assigned_TC,
    u2.email AS 'TC email',
    of2.name as TC_Assigned_Office,
    u1.id AS AgentID,
    u1.first_name AS 'Agent_First_Name',
    u1.last_name AS 'Agent Last Name',
    u1.email AS 'Agent_email',
    u1.phone AS 'Agent_Phone',
    office.name AS 'Brokerage',
    office.agreement_type AS 'Agreement_Type',
    if(u1.pays_at_title = 0,'N','Y') as PaysAtTitle,
    line_item.order_id AS 'Order_Id',
    line_item.id AS 'LineItemId'

FROM
    line_item
    JOIN user u1 ON u1.id = line_item.user_id
    JOIN tc_order on line_item.order_id = tc_order.id
    LEFT JOIN office of2 ON tc_order.assigned_tc_office_id = of2.id
    JOIN transaction on tc_order.transaction_id = transaction.id
    JOIN user u2 ON tc_order.assigned_tc_id = u2.id
    LEFT JOIN office_user ON office_user.user_id = u1.id
    LEFT JOIN office ON office_user.office_id = office.id

WHERE 
    line_item.description in ('Listing Coordination Fee','Transaction Coordination Fee')
    and line_item.status not in ('withdrawn','cancelled')
    and line_item.paid = 0

group by 
    line_item.due_date

order by 
    line_item.status, line_item.due_date, line_item.created




-- buyer information for connections
SELECT
    transaction.id as transaction_id,
    tc_order.address_line_1 as address_line_1,
    date(contract.closing_date) as closing_date,
    datediff(contract.closing_date, now()) as days_to_close,
    CASE
        WHEN ( ttv_utility.internal_name is not null ) THEN 'LEAD SENT'
        WHEN (
        transaction.id in (select transaction_id from member join user on member.user_id = user.id join user_transactly_vendor_opt_out on member.user_id = user_transactly_vendor_opt_out.user_id where member.role_id = 7 and
        user_transactly_vendor_opt_out.vendor_type_id = 10
        )
        ) THEN 'AGENT OPTED OUT'
        WHEN (
        transaction.id not in (select transaction_id from contact where contact.role_id = 6) and
        transaction.id not in (select transaction_id from member where member.role_id = 6)
        ) THEN 'NO BUYER'
        WHEN (
        transaction.id not in (select transaction_id from contact where contact.role_id = 6 and COALESCE(contact.email, '') != '' and COALESCE(contact.phone, '') != '') and
        transaction.id not in (select transaction_id from member join user on member.user_id = user.id where member.role_id = 6 and COALESCE(user.email, '') != '' and COALESCE(user.phone, '') != '')
        ) THEN 'INCOMPLETE BUYER'
        ELSE 'READY FOR SENDING'
        END as utility_transfer_status,
    ttv_utility.internal_name as utility_lead_sent_to,
    DATE(ttv_utility.notified_date) as utility_notified_date,
    CASE
        WHEN ( ttv_home_insurance.internal_name is not null ) THEN 'LEAD SENT'
        WHEN (
        transaction.id in (select transaction_id from member join user on member.user_id = user.id join user_transactly_vendor_opt_out on member.user_id = user_transactly_vendor_opt_out.user_id where member.role_id = 7 and
        user_transactly_vendor_opt_out.vendor_type_id = 7
        )
        ) THEN 'AGENT OPTED OUT'
        WHEN (
        transaction.id not in (select transaction_id from contact where contact.role_id = 6) and
        transaction.id not in (select transaction_id from member where member.role_id = 6)
        ) THEN 'NO BUYER'
        WHEN (
        transaction.id not in (select transaction_id from contact where contact.role_id = 6 and COALESCE(contact.email, '') != '' and COALESCE(contact.phone, '') != '') and
        transaction.id not in (select transaction_id from member join user on member.user_id = user.id where member.role_id = 6 and COALESCE(user.email, '') != '' and COALESCE(user.phone, '') != '')
        ) THEN 'INCOMPLETE BUYER'
        ELSE 'READY FOR SENDING'
        END as home_insurance_status,
    ttv_home_insurance.internal_name as home_insurance_lead_sent_to,
    DATE(ttv_home_insurance.notified_date) as home_insurance_notified_date,
    u1.email as agent_email, u1.first_name as agent_first_name, u1.last_name as agent_last_name, u1.phone as agent_phone,
    u2.email as tc_email, u2.first_name as tc_first_name, u2.last_name as tc_last_name, u2.phone as tc_phone,
    buyer_info.first_name as buyer_first_name, buyer_info.last_name as buyer_last_name, buyer_info.email as buyer_email, buyer_info.phone as buyer_phone

FROM
    transaction
    join contract on contract.id = transaction.current_contract_id
    join tc_order on tc_order.transaction_id = transaction.id
    join user u1 on tc_order.agent_id = u1.id
    join user u2 on tc_order.assigned_tc_id = u2.id
    left outer join (
    select * from (
        select
            member.transaction_id as transaction_id,
            user.first_name as first_name,
            user.last_name as last_name,
            user.email as email,
            user.phone as phone,
            ((case when coalesce(user.phone, '') = '' then 0 else 1 end) + (case when coalesce(user.email, '') = '' then 0 else 1 end)) as contact_methods
        from member join user on member.user_id = user.id
        where member.role_id = 6

        UNION ALL
        select
            contact.transaction_id as transaction_id,
            contact.first_name as first_name,
            contact.last_name as last_name,
            contact.email as email,
            contact.phone as phone,
            ((case when coalesce(contact.phone, '') = '' then 0 else 1 end) + (case when coalesce(contact.email, '') = '' then 0 else 1 end)) as contact_methods
        from contact
        where contact.role_id = 6

    ) as buyers group by transaction_id
    ) as buyer_info on buyer_info.transaction_id = transaction.id
    left outer join (select ttv.transaction_id, ttv.notified_date, tv.internal_name from transaction_transactly_vendor ttv join transactly_vendor tv on ttv.transactly_vendor_id = tv.id and tv.vendor_type_id = 10) as ttv_utility on ttv_utility.transaction_id = transaction.id
    left outer join (select ttv.transaction_id, ttv.notified_date, tv.internal_name from transaction_transactly_vendor ttv join transactly_vendor tv on ttv.transactly_vendor_id = tv.id and tv.vendor_type_id = 7) as ttv_home_insurance on ttv_home_insurance.transaction_id = transaction.id

WHERE
    transaction.status_id != 6 and
    transaction.status_id != 7 and
    tc_order.side_id = 1 and
    contract.closing_date is not null and
    datediff(contract.closing_date, now()) between <Parameters.Start> and <Parameters.End>
    
GROUP BY 
    transaction.id    