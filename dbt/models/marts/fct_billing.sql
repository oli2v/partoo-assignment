with payed_amounts_sum as (
    select
        payment_date,
        sum(payed_amount) as payed_amount
    from {{ ref("dim_business") }}
    group by payment_date
), invoiced_amounts_sum as (
    select
        invoice_date,
        sum(invoiced_amount) as invoiced_amount
    from {{ ref("dim_business") }}
    group by invoice_date
), billing_ts as (
    select
        full_date as date,
        year,
        year_week,
        month,
        fiscal_qtr as quarter,
        week_day,
        coalesce(payed_amount, 0) as payed_amount,
        coalesce(invoiced_amount, 0) as invoiced_amount
    from {{ source("operations", "date_dim" )}} d
    left join payed_amounts_sum pas on d.full_date = pas.payment_date
    left join invoiced_amounts_sum ias on d.full_date = ias.invoice_date
)
select *
from billing_ts
where date between (select min(invoice_date) from {{ ref("stg_invoice") }}) 
and (select max(invoice_date) from {{ ref("stg_invoice") }})
order by date desc
