with stripe_payments as (
    select
         id                               as payment_id,
         object,
         cast(amount as number(20,2))     as amount,
         currency,
         customer                         as customer_id,
         payment_method                   as payment_method_id,
         status,
         {{convert_timestamp('created')}} as created_date,
         livemode
    from {{source('mysql_raw_data','stripe_payments')}}
)

select * from stripe_payments