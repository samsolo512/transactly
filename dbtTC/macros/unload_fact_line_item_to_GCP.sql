-- https://github.com/dbt-labs/dbt-snowflake/issues/169

{% macro unload_fact_line_item_to_GCP() %}

{% call statement('load_GCP_fact_line_item', fetch_result=true, auto_begin=true) %}

    -- begin;
        copy into @GCP_stage/GCP_fact_line_item
        from GCP_fact_line_item
        overwrite = true
        single = true;
    -- commit;

{% endcall %}

{% endmacro %}
