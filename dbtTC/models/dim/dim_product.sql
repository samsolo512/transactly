with
    src_sf_product as(
        select *
        from {{ ref('src_sf_product_2')}}
    )

    ,final as(
        select
            working.seq_dim_product.nextval as product_pk
            ,p.product_id
            ,p.product_name
            ,p.product_family
        from
            src_sf_product_2 p
    )

select * from final