order_payments as (
    select
        order_id,
        sum(case when status = 'success' then amount end) as amount
    from {{ ref('stg_payments') }}
    group by 1
),
select 
    o.order_id as order_id,
    p.customer_id as customer_id,
    o.order_date
    coalesce(o.amount, 0) as amount 
from {{ ref('stg_orders') }} as o
left join order_payments as p  
    on (o.order_id = p.order_id)