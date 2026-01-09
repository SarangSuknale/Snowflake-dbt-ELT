with stripe_price as (
    select 
          id                                      as price_id,
          object,
          product                                 as product_id,
          unit_amount                             as price,
          currency,
          parse_json(recurring):interval::varchar as interval,
          active                                  as is_active,
          {{convert_timestamp('created')}}        as created_date
    from {{source('mysql_raw_data','stripe_price')}}
)

select * from stripe_price