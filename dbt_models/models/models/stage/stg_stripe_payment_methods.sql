with stripe_payment_methods as (
    select
          id                                    as payment_method_id,
          object,
          customer                              as customer_id,
          type,
          parse_json(card):brand::varchar       as card_brand,
          parse_json(card):last4::number(4)     as card_last_four_digit,
          parse_json(card):exp_month::number(2) as exp_month,
          parse_json(card):exp_year::number(4)  as exp_year,
          {{convert_timestamp('created')}}      as created_date,
          livemode
    from {{source('mysql_raw_data','stripe_payment_methods')}}
)

select * from stripe_payment_methods