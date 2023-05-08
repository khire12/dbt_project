select 
    id,
    order_id,
    paymentmethod,
    status_,
    amount/100 as amount,
    created
from {{ source('stripe', 'stripe_payments') }}
