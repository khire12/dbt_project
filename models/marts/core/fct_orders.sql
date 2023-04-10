with order_payments as (
    select
        order_id,
        sum(case when status_ = 'success' then amount end) as amount
    from {{ ref('stg_payments') }}
    group by 1
)
select 
    o.order_id as order_id,
    o.customer_id as customer_id,
    o.order_date,
    coalesce(p.amount, 0) as amount 
from {{ ref('stg_orders') }} as o
left join order_payments as p  
    on (o.order_id = p.order_id)