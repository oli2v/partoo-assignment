select 
    *,
    date(created_at) as subscription_date,
    DATE_DIFF(churn_date, date(created_at), month) as months_before_churn
from {{ source("operations", "business") }}