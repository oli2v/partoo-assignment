select
    b.business_id,
    status,
    invoice_date,
    invoiced_amount,
    payment_date,
    payed_amount,
    round(payed_amount / invoiced_amount, 2) as payed_ratio,
    round(invoiced_amount - p.payed_amount, 2) as discrepancy,
    DATE_DIFF(payment_date, invoice_date, day) as payment_time,
    months_before_churn
from
    {{ ref("stg_business") }} b
left join {{ ref("stg_invoice") }} i on b.business_id = i.business_id
left join {{ ref("stg_payment") }} p on b.business_id = p.business_id