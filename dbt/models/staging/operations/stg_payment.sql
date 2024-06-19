select 
    business_id,
    amount as payed_amount,
    payment_date
from {{ source("operations", "payment") }}