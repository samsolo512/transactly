{% macro field_clean(field_name) %}

    (regexp_replace(
        regexp_replace(
            {{ field_name }},
            '[\r\n]',  -- replace line returns with a space
            ' '
        ),
        '[\"]',  -- replace quotation marks with no space
        ''
    ))

{% endmacro %}
