
with payment as (
    select
          payment_id,
          customer_id,
          payment_method_id,
          amount,
          currency,
          status,
          created_date,
          datediff('day', created_date, current_date()) as days_since_payment,
          datediff('month', created_date, current_date()) as months_since_payment
    from {{ref('stg_stripe_payments')}}
),

payment_methods as (
    select
          payment_method_id,
          customer_id,
          type,
          card_brand,
          card_last_four_digit,
          exp_month,
          exp_year,
          created_date,
          datediff('day', created_date, current_date()) as days_since_card_added,
          datediff('month', created_date, current_date()) as months_since_card_added
    from {{ref('stg_stripe_payment_methods')}}
),

final as (
    select 
          p.payment_id,
          pm.payment_method_id,
          pm.customer_id,
          p.amount,
          p.currency,
          p.status,
          p.created_date as payment_made_date,
          p.days_since_payment,
          p.months_since_payment,
          pm.type,
          pm.card_brand,
          pm.card_last_four_digit,
          pm.exp_month,
          pm.exp_year,
          pm.created_date as card_added_date,
          pm.days_since_card_added,
          pm.months_since_card_added
    from payment p
    inner join payment_methods pm
    on p.payment_method_id=pm.payment_method_id
)

select * from final