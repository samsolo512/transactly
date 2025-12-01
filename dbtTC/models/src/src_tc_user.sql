with 
    src_tc_user as(
        select *
        from {{ source('gcp_prod_gcp_prod_prod', 'user') }}
        where lower(_fivetran_deleted) = 'false'
    )

    ,base as(
        select
            u.id as user_id
            ,u.join_date
            ,u.is_active
            ,u.is_tc_client
            ,u.assigned_transactly_tc_id
            ,cast(u.last_online_date as date) as last_online_date
            ,trim({{ field_clean('u.first_name') }}) as first_name
            ,trim({{ field_clean('u.last_name') }}) as last_name
            ,trim(concat({{ field_clean('u.first_name') }}, ' ', {{ field_clean('u.last_name') }})) as fullname
            ,trim({{ field_clean('u.email') }}) as email
            {# ,regexp_replace(email, '[^A-Z0-9a-z\@\.\_\\-\+\`\ \,\/\\[\\]\=\)\;\>]', '') as email #}
            {# ,regexp_replace({{ field_clean('u.email') }}, '[^A-Z0-9a-z\@\.\_\\-\+]', '') as email #}
            ,u.first_login
            ,case when u.auto_payment = true then 1 else 0 end as auto_payment_flag
            ,cast(u.autopay_date as date) as autopay_date
            ,cast(u.created as date) as created_date
            ,u.google_user_id
            ,u.pays_at_title
            ,u.brokerage
            ,u.self_procured
            ,u.phone
            ,u.stripe_account_id
            ,u.customer_id
            ,to_date(u.updated) as updated_date
        from
            src_tc_user u
    )

    /*
    Some emails are entered more than once and the dup email usually has a leading or trailing space.
    Since the function field_clean is used to trim spaces, we can have multiple records with the same email after trimming.
    To handle this, we will keep the most recently created record for each email.
    */
    ,final as(
        select
            base.*
            ,row_number() over(partition by base.email order by base.created_date desc) as rn
        from base
        qualify rn = 1
    )

select * from final
