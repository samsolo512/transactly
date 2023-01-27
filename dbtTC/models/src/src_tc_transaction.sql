with
    src_tc_transaction as(
        select *
        from {{ source('gcp_prod_gcp_prod_prod', 'transaction') }}
        where lower(_fivetran_deleted) = 'false'
    )

    ,final as(
        select
            t.id as transaction_id
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
            ,t.short_url
        from
            src_tc_transaction t
        where
            t._fivetran_deleted = 'FALSE'
    )

select * from final
