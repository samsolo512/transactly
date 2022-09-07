-- https://github.com/dbt-labs/dbt-snowflake/issues/169

{% macro unload_to_GCP(table_name) %}

{% call statement('load_GCP_dim_lead', fetch_result=true, auto_begin=true) %}

    -- begin;
        copy into @GCP_stage/{{table_name}}
        from {{table_name}}
        overwrite = true
        single = true;
    -- commit;

{% endcall %}

{% endmacro %}
