with customers as (

    select * from {{ ref('stg_customers')}}

),

orders as (

    select * from {{ ref('fct_orders') }}

),
payments as (

    select * from {{ ref('stg_payments') }}

),

order_payments as (

    select 
        o.customer_id,
        sum(p.amount) as lifetime_value2
    from  {{ ref('stg_orders') }} as o
    left join {{ ref('stg_payments') }} as p 
        on (o.order_id = p.order_id)
    group by 1
),


customer_orders as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders,
        sum(amount) as limetime_value
    from orders
    group by 1
),
final as (
    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        order_payments.lifetime_value2,
        customer_orders.limetime_value as limetime_value
    from customers

    left join customer_orders 
        on customers.customer_id = customer_orders.customer_id
    left join order_payments 
        on customers.customer_id = order_payments.customer_id
)

select * from final