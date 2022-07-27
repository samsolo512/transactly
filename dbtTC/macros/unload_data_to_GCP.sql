-- https://github.com/dbt-labs/dbt-snowflake/issues/169

{% macro unload_data_to_GCP() %}

{% call statement('load_GCP_fact_order', fetch_result=true, auto_begin=true) %}

    -- begin;
        copy into @GCP_stage/GCP_fact_line_item
        from GCP_fact_line_item
        overwrite = true
        single = true;
    -- commit;

{% endcall %}

{% endmacro %}

-- dbt run-operation unload_data_to_GCP