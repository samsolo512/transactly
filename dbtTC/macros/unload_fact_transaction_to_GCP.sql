-- https://github.com/dbt-labs/dbt-snowflake/issues/169

{% macro unload_fact_transaction_to_GCP() %}

{% call statement('load_GCP_fact_transaction', fetch_result=true, auto_begin=true) %}

    -- begin;
        copy into @GCP_stage/GCP_fact_transaction
        from GCP_fact_transaction
        overwrite = true
        single = true;
    -- commit;

{% endcall %}

{% endmacro %}
