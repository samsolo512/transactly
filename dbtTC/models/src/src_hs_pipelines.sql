-- src_hs_pipeline

with src_hs_pipelines as(
    select *
    from {{ source('hs', 'pipelines') }}
)

select
    pipelineid
    ,label
from 
    src_hs_pipelines
