with finicity_transactions as (
    select
           acc.value:account_id::varchar(100)                                      as acccount_id,
           variant_col:user_id::varchar(100)                                       as user_id,
           txt.value:id::varchar(100)                                              as transaction_id,
           {{convert_date('txt.value:transaction_date::varchar(100)')}}            as transaction_date,
           txt.value:amount::number(20,2)                                          as amount,
           txt.value:category::varchar(100)                                        as category,
           txt.value:merchant::varchar(100)                                        as merchant,
           txt.value:currency::varchar(10)                                         as currency,
           txt.value:daily_alert_email_sent::varchar(50)                           as daily_alert_email_sent,
           {{convert_date('txt.value:daily_alert_email_sent_date::varchar(100)')}} as daily_alert_email_sent_date,
           txt.value:daily_alert_sent::varchar(50)                                 as daily_alert_sent,
           {{convert_date('txt.value:daily_alert_sent_date::varchar(100)')}}       as daily_alert_sent_date,
           txt.value:description::varchar(500)                                     as description,
           txt.value:is_manually_added::varchar(50)                                as is_manually_added,
           txt.value:is_recurring::varchar(50)                                     as is_recurring,
           txt.value:is_verified::varchar(50)                                      as is_verified,
           txt.value:memo::varchar(500)                                            as memo,
           txt.value:modified_category::varchar(100)                               as modified_category,
           txt.value:modified_merchant::varchar(100)                               as modified_merchant,
           {{convert_date('txt.value:post_date::varchar(100)')}}                   as post_date,
           txt.value:status::varchar(100)                                          as transaction_status,
           txt.value:tag::varchar(500)                                             as tag,
           txt.value:transaction_modified::varchar(100)                            as transaction_modified,
           txt.value:type::varchar(100)                                            as type,
           {{convert_date('txt.value:updated_on::varchar(100)')}}                  as updated_on,
           txt.value:was_pending::varchar(100)                                     as was_pending
    from {{source('s3_raw_data','finicity_transactions')}},
    lateral flatten(input=>variant_col:accounts) as acc,
    lateral flatten(input=>acc.value:transactions) as txt
)

select * from finicity_transactions