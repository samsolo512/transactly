
{{ config(
    post_hook=[
      "{{unload_to_GCP('GCP_fact_transaction')}}"
    ]
) }}

with
    fact_transaction as(
        select *
        from {{ ref('fact_transaction') }}
    )

    ,dim_transaction as(
        select *
        from {{ ref('dim_transaction') }}
    )

    ,dim_user as(
        select *
        from {{ ref('dim_user') }}
    )

select
    usr.user_id
    ,tran.transaction_id
    ,usr.fullname
    ,usr.brokerage
    ,tran.created_date
    ,tran.closed_date
    ,tran.diy_flag
from
    fact_transaction fact
    left join dim_transaction tran on fact.transaction_pk = tran.transaction_pk
    join dim_user usr on fact.user_pk = usr.user_pk
