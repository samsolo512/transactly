-- HS_opportunity

{{
    config(
        materialized = 'table'
    )
}}

with
    src_hs_pipeline_stages as(
        select *
        from {{ ref('src_hs_pipeline_stages')}}
    )

    ,src_hs_object_properties as(
        select *
        from {{ ref('src_hs_object_properties')}}
    )

    ,src_hs_owners as(
        select *
        from {{ ref('src_hs_owners')}}
    )

    ,HS_refine as(
        select distinct
            objectid
        from
            src_hs_object_properties
        where
            name = 'opportunity_id'
            and value is not null
    )

    ,starting as(
        select 
            a.objectid
            ,a.name
            ,a.value 
        from 
            src_hs_object_properties a
            join HS_refine b on a.objectid = b.objectid
    )

    ,HS_pivot as(
        select 
            objectid
            ,opportunity_name
            ,opportunity_id
            ,stage
            ,lease_start_date
            ,opportunity_owner_id
            ,product_family
            ,address
        from 
            starting hs
            
            pivot(
                max(value) for name in(
                    'dealname'
                    ,'opportunity_id'
                    ,'dealstage'
                    ,'lease_begins__c'
                    ,'hubspot_owner_id'
                    ,'product_families__c'
                    ,'customer_address'
                )
            ) as p (
                objectid
                ,opportunity_name
                ,opportunity_id
                ,stage
                ,lease_start_date
                ,opportunity_owner_id
                ,product_family
                ,address
            )
    )

    ,final as(
        select
            -- p.objectid
            p.opportunity_name
            ,p.opportunity_id
            ,ps.label as stage
            ,concat(o.firstname, ' ', o.lastname) as owner_name
            ,try_to_date(p.lease_start_date) as lease_start_date
            ,p.product_family
            ,p.address
            ,o.email as agent_email
        from
            HS_pivot p
            left join src_HS_pipeline_stages ps on p.stage = ps.stageid
            left join src_HS_owners o on p.opportunity_owner_id = to_varchar(o.ownerid)
    )

select * from final
