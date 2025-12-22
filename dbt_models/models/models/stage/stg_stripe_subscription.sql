with stripe_sub as (
    select 
          id as subscription_id,
          object,
          customer as customer_id,
          price_id,
          quantity,
          status,
          {{convert_timestamp('current_period_start')}} as current_period_start,
          {{convert_timestamp('current_period_end')}} as current_period_end,
          {{convert_timestamp('created')}} as created_date,
          livemode
    from {{source('mysql_raw_data','stripe_subscriptions')}}
)

select * from stripe_sub