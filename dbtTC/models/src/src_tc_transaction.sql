with
    src_tc_transaction as(
        select *
        from {{ source('tc', 'transaction') }}
        where lower(_fivetran_deleted) = 'false'
    )

    ,src_tc_address as(
        select *
        from {{ source('tc', 'address') }}
        where lower(_fivetran_deleted) = 'false'
    )

    ,no_dups as(
        select
            a.address_line_1 as street
            ,a.city
            ,a.state
            ,max(t.status_changed_date) as status_changed_date
        from
            src_tc_transaction t
            join src_tc_address a on t.address_id = a.id
        where a._fivetran_deleted = 'FALSE'
        group by address_line_1, city, state
    )

    ,final as(
        select
            t.id as transaction_id
            ,t.created_by_id as user_id
            ,t.status_id
            ,t.type_id
            ,t.category_id
            ,t.side_id
            ,cast(t.created as date) as created_date
            ,cast(t.closed_date as date) as closed_date
            ,t.created_by_id
            ,t.address_id
            ,t.expiration_date
            ,t.current_contract_id
            ,t.status_changed_date
        from
            src_tc_transaction t
            join src_tc_address a on t.address_id = a.id
            join no_dups nd
                on a.address_line_1 = nd.street
                and a.city = nd.city
                and a.state = nd.state
                and t.status_changed_date = nd.status_changed_date
        where
            t._fivetran_deleted = 'FALSE'
            and a._fivetran_deleted = 'FALSE'
    )

select * from final
