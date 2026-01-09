with stripe_cust as (
    select
          id                               as customer_id,
          object,
          {{convert_timestamp('created')}} as created_date,
          email,
          name,
          livemode,
          user_id
    from {{source('mysql_raw_data','stripe_customers')}}
)

select * from stripe_cust