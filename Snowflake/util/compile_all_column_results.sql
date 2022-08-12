select *
from "FIVETRAN"."INFORMATION_SCHEMA".columns
where
    lower(column_name) like '%contact%'
    and lower(table_schema) = 'transactly_app_production_rec_accounts'
;






create or replace table dev.working.all_cols(table_name varchar, column_name varchar, value varchar);


execute immediate $$
declare
    sql_statement varchar;
    final_sql varchar;
    res resultset;

    c1 cursor for(
        select distinct
            table_name
            ,column_name
//            ,'select distinct ' || '\'' || table_name || '\'' || ', ' || '\'' || column_name || '\', ' || column_name || ' from FIVETRAN.TRANSACTLY_APP_PRODUCTION_REC_ACCOUNTS.' || table_name || ' ' as sql_query
            ,'select distinct ' || '\'' || table_name || '\'' || ', ' || '\'' || column_name || '\', ' || column_name || ' from FIVETRAN.SALESFORCE.' || table_name || ' ' as sql_query
        from "FIVETRAN"."INFORMATION_SCHEMA".columns
        where
//            table_schema = 'TRANSACTLY_APP_PRODUCTION_REC_ACCOUNTS'
            table_schema = 'SALESFORCE'
            and column_name not like '%FIVETRAN%'
            and table_name not like '%FIVETRAN%'
            and data_type in('TEXT')
        order by 1,2
    );

begin
    final_sql := ' ';

    open c1;
    for record in c1 do
        sql_statement := record.sql_query;

        //https://stackoverflow.com/questions/71684411/perform-snowflake-sql-queries-in-for-loop-based-on-data-from-another-table
        if(final_sql = ' ') then
            final_sql := 'insert into dev.working.all_cols ';
        else
            final_sql := final_sql || ' union all ';
        end if;

        final_sql := final_sql || sql_statement;

    end for;

    res := (execute immediate :final_sql);
    return table(res);
//    return final_sql;
end;
$$
;




select distinct table_name, column_name
from dev.working.all_cols
where
//    lower(value) like '%electric-4change energy-36 month%'
    lower(value) like '%owner%'
order by 1,2
;
