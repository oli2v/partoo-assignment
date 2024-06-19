select
    business_id,
    invoiced_amount,
    created_at as invoice_date
from {{ source("operations", "invoice") }}