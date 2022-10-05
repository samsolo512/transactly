{{ config(
    post_hook=[
      "{{unload_to_GCP('GCP_Sugar_contacts')}}"
    ]
) }}


with
    src_Sugar_contacts as(
        select *
        from {{ ref('src_Sugar_contacts') }}
    )

select *
from src_Sugar_contacts
