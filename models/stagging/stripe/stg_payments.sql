select 
    id,
    order_id,
    paymentmethod,
    status_,
    amount/100 as amount,
    created
from raw.jaffle_shop.stripe_payments
