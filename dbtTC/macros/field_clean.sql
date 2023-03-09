{% macro field_clean(field_name) %}

    (
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    {{ field_name }},
                    '[\r\n]',  -- replace these items with a space
                    ' '
                ),
                '[\"\^]',  -- replace these items with no space
                ''
            ),
            '[^a-zA-Z0-9\@\.\_\+\>\,\-\;\'\=\`]'  -- get rid of everything that arent these characters
        )
    )

{% endmacro %}


