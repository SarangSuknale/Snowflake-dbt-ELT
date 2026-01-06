
with subscription as (
    select
          subscription_id,
          customer_id,
          price_id,
          quantity,
          status,
          current_period_start,
          current_period_end,
          case
              when current_date() < current_period_end then datediff('day', current_date(), current_period_end)
              else 0
          end as current_period_remaining_days,
          case
              when current_date() < current_period_end then datediff('month', current_date(), current_period_end)
              else 0
          end as current_period_remaining_months,
          created_date
    from {{ref('stg_stripe_subscription')}}
)

select * from subscription