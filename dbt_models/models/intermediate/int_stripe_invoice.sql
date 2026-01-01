with invoice as (
    select
          invoice_id,
          customer_id,
          object,
          subscription_id,
          amount_due,
          currency,
          status,
          created_date,
          datediff('day', created_date, current_date()) as days_since_created,
          datediff('month', created_date, current_date()) as months_since_created
    from {{ref('stg_stripe_invoice')}}
)

select * from invoice