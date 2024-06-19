select 
    *,
    date(created_at) as subscription_date 
from {{ source("operations", "business") }}