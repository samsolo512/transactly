-- https://github.com/dbt-labs/dbt-snowflake/issues/169

{% macro unload_dim_lead_to_GCP() %}

{% call statement('load_GCP_dim_lead', fetch_result=true, auto_begin=true) %}

    -- begin;
        copy into @GCP_stage/GCP_dim_lead
        from GCP_dim_lead
        overwrite = true
        single = true;
    -- commit;

{% endcall %}

{% endmacro %}
