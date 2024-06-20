select 
    business_id,
    amount as payed_amount,
    payment_date as payment_datetime,
    date(payment_date) as payment_date
from {{ source("operations", "payment") }}