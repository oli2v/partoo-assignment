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
)
select *
from subscription_ts
where date <= current_date()
order by date desc

-- with subscription_ts as (
--     select
--         subscription_date,
--         count(distinct business_id) as subscription_count
--     from {{ source("operations", "date_dim" )}} d
--     left join {{ ref("stg_business") }} b on d.full_date = b.subscription_date
--     group by subscription_date
-- ), churn_ts as (
--     select
--         churn_date,
--         count(distinct business_id) as churn_count
--     from {{ source("operations", "date_dim") }} d
--     left join {{ ref("stg_business") }} b on d.full_date = b.churn_date
--     group by churn_date
-- ), joined_ts as (
--     select
--         subscription_date as date,
--         coalesce(subscription_count, 0) as subscription_count,
--         coalesce(churn_count, 0) as churn_count
--     from subscription_ts sts
--     left join churn_ts cts on sts.subscription_date = cts.churn_date
-- )