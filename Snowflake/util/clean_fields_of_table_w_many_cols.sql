 select
    column_name
    ,data_type
    ,concat(
        ',{{ field_clean(\047"'
        ,column_name
        ,'"\047) }} as '
        ,column_name
        -- ,case
        --     when data_type = 'TEXT' then 'string'
        --     when data_type like 'TIME%' then 'datetime'
        --     when data_type = 'DATE' then 'date'
        --     when data_type like 'BOOL%' then 'int'
        --     when data_type like 'NUM%' then 'numeric'
        -- else null
        -- end
    )
from skyvia.INFORMATION_SCHEMA.COLUMNS
where
    lower(table_name) = 'contacts'
order by ordinal_position
;