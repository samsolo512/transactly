-- https://github.com/dbt-labs/dbt-snowflake/issues/169

{% macro unload_fact_opportunity_to_GCP() %}

{% call statement('load_GCP_fact_opportunity', fetch_result=true, auto_begin=true) %}

    -- begin;
        copy into @GCP_stage/GCP_fact_opportunity
        from GCP_fact_opportunity
        overwrite = true
        single = true;
    -- commit;

{% endcall %}

{% endmacro %}
