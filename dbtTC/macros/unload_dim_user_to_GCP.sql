-- https://github.com/dbt-labs/dbt-snowflake/issues/169

{% macro unload_dim_user_to_GCP() %}

{% call statement('load_GCP_dim_user', fetch_result=true, auto_begin=true) %}

    -- begin;
        copy into @GCP_stage/GCP_dim_user
        from GCP_dim_user
        overwrite = true
        single = true;
    -- commit;

{% endcall %}

{% endmacro %}
