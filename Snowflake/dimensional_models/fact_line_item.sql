-- fact_line_item
-- 1 row/line_item

select
    *
from
    fact_line_item fact
    join dim_line_item line on fact.line_item_pk = line.line_item_pk
    join dim_user user on fact.user_pk = user.user_pk
    join dim_order o on fact.order_pk = o.order_pk
    join dim_date created_date on fact.created_date_pk = created_date.date_pk
    join dim_date due_date on fact.due_date_pk = due_date.date_pk
    join dim_date cancelled_date on fact.created_date_pk = cancelled_date.date_pk
;