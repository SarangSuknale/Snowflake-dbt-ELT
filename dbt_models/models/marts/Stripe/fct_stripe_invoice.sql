
with invoice as (
    select
          invoice_id,
          customer_id,
          subscription_id,
          amount,
          currency,
          status,
          created_date,
          days_since_created,
          months_since_created
    from {{ref('int_stripe_invoice')}}
)

select * from invoice