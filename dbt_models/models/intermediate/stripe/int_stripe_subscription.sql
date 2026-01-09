
with subscription as (
    select
          subscription_id,
          customer_id,
          price_id,
          quantity,
          status,
          status = 'active' as is_active_subscription,
          current_period_start,
          current_period_end,
          current_period_end < current_date() as is_expired,
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