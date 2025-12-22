with plaid_transactions as (
    select
          acc.value:account_id::varchar(50)                           as acccount_id,
          txt.value:transaction_id::varchar(100)                      as transaction_id,
          txt.value:amount::number(20,2)                              as amount,
          {{convert_date('txt.value:authorized_date::varchar(100)')}} as authorized_date,
          txt.value:category::varchar(100)                            as category,
          nullif(txt.value:category_id::varchar(50), '')              as category_id,
          {{convert_date('txt.value:date::varchar(100)')}}            as transaction_date,
          txt.value:iso_currency_code::varchar(10)                    as currency,
          txt.value:item_id::varchar(100)                             as item_id,
          txt.value:merchant_name::varchar(100)                       as merchant_name,
          nullif(txt.value:merchant_type::varchar(100), '')           as merchant_type,
          txt.value:name::varchar(100)                                as name,
          txt.value:payment_channel::varchar(100)                     as payment_channel,
          txt.value:pending::varchar(100)                             as is_pending,
          txt.value:transaction_type::varchar(100)                    as transaction_type
    from {{source('s3_raw_data','plaid_transactions')}},
    lateral flatten(input=>variant_col:accounts) as acc,
    lateral flatten(input=>acc.value:transactions) as txt
)

select * from plaid_transactions