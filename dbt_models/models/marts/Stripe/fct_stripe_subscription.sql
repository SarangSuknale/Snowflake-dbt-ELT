
with strip_sub as (
    select
          subscription_id,
          customer_id,
          price_id,
          quantity,
          status,
          current_period_start,
          current_period_end,
          current_period_remaining_days,
          current_period_remaining_months,
          is_active_subscription,
          is_expired,
          created_date
    from {{ref('int_stripe_subscription')}}
)

select * from strip_sub