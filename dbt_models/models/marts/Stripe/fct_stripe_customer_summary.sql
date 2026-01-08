
with payments_agg as (

    select
        customer_id,
        count(*) as total_payments,
        sum(amount) as total_revenue,
        max(payment_made_date) as last_payment_date,
        avg(amount)::number(10,2) as avg_payment_amount
    from {{ ref('int_stripe_payment') }}
    where is_successful_payment
    group by customer_id

),

invoices_agg as (

    select
        customer_id,
        count(*) as total_invoices,
        sum(amount) as total_invoiced_amount,
        sum(case when status = 'open' then amount else 0 end) as outstanding_amount,
        max(created_date) as last_invoice_date
    from {{ ref('int_stripe_invoice') }}
    group by customer_id

)

select
    c.customer_id,
    coalesce(p.total_payments, 0) as total_payments,
    coalesce(p.total_revenue, 0) as total_revenue,
    p.avg_payment_amount,
    p.last_payment_date,
    coalesce(i.total_invoices, 0) as total_invoices,
    coalesce(i.total_invoiced_amount, 0) as total_invoiced_amount,
    coalesce(i.outstanding_amount, 0) as outstanding_amount,
    i.last_invoice_date
from {{ ref('int_stripe_customers') }} c
left join payments_agg p on c.customer_id = p.customer_id
left join invoices_agg i on c.customer_id = i.customer_id
