with stripe_invoice as (
    select
          id                               as invoice_id,
          object,
          customer                         as customer_id,
          subscription                     as subscription_id,
          cast(amount_due as number(10,2)) as amount_due,
          currency,
          status,
          {{convert_timestamp('created')}} as created_date,
          livemode
    from {{source('mysql_raw_data','stripe_invoice')}}
)

select * from stripe_invoice