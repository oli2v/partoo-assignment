with distinct_sub_count as (
    select
        subscription_date,
        count(distinct business_id) as subscription_count
    from {{ ref("stg_business") }}
    group by subscription_date
), distinct_churn_count as (
    select
        churn_date,
        count(distinct business_id) as churn_count
    from {{ ref("stg_business") }}
    group by churn_date
), subscription_ts as (
    select
        full_date as date,
        year,
        year_week,
        month,
        fiscal_qtr as quarter,
        week_day,
        coalesce(subscription_count, 0) as subscription_count,
        coalesce(churn_count, 0) as churn_count
    from {{ source("operations", "date_dim" )}} d
    left join distinct_sub_count dsc on d.full_date = dsc.subscription_date
    left join distinct_churn_count dcc on d.full_date = dcc.churn_date
), cumsum as (
    select
        *,
        sum(subscription_count) over (order by date asc) as subscription_count_cumsum,
        sum(churn_count) over (order by date asc) as churn_count_cumsum,
    from subscription_ts
    where date between (select min(subscription_date) from {{ ref("stg_business") }}) 
    and (select max(subscription_date) from {{ ref("stg_business") }})
)
select
    *,
    round(churn_count_cumsum /  subscription_count_cumsum, 2) as churn_rate
from cumsum
order by date desc