-- src_hs_pipeline_stages

with src_hs_pipeline_stages as(
    select *
    from {{ source('hs', 'pipeline_stages') }}
)

select
    stageid
    ,label
from 
    src_hs_pipeline_stages
