{% macro field_clean(field_name) %}

    (regexp_replace(
        regexp_replace(
            {{ field_name }},
            '[\r\n]',  -- replace these items with a space
            ' '
        ),
        '[\"\^]',  -- replace these items with no space
        ''
    ))

{% endmacro %}

-- a.address_line_1 regexp '.*[^a-zA-Z0-9 \.()#,\-\/\'{}:&\*_@`].*'
