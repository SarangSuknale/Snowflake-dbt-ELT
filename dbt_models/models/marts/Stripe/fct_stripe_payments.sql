
with payments as (
    select
          payment_id,
          customer_id,
          amount,
          currency,
          status,
          is_successful_payment,
          payment_made_date,
          days_since_payment,
          months_since_payment,
          payment_method_type,
          card_brand,
          card_last_four_digit,
          exp_month,
          exp_year,
          card_added_date,
          days_since_card_added,
          months_since_card_added
    from {{ref('int_stripe_payment')}}
)

select * from payments