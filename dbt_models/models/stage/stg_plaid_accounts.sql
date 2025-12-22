with plaid_accounts as (
    select
        cast(id as varchar(100))                          as account_id,
        cast(user_id as varchar(100))                     as user_id,
        cast(providerid as varchar(100))                  as provider_id,
        cast(providername as varchar(255))                as provider_name,
        {{convert_timestamp('created_date')}}             as created_date,
        {{convert_timestamp('last_updated')}}             as last_updated,
        cast(aggregationsource as varchar(100))           as aggregation_source,
        cast(accountname as varchar(200))                 as account_name,
        cast(accountnumber as number(20,0))               as account_number,
        cast(container as varchar(100))                   as container,
        cast(accounttype as varchar(100))                 as account_type,
        parse_json(balance):amount::number(10,2)          as amount,
        parse_json(balance):currency::varchar(10)         as currency,
        parse_json(availablebalance):amount::number(10,2) as available_balance,
        parse_json(currentbalance):amount::number(10,2)   as current_balance,
        parse_json(amountdue):amount::number(10,2)        as amount_due,
        to_timestamp(lastrefreshed)                       as last_refreshed,
        to_timestamp(lastrefreshattempt)                  as last_refresh_attempt,
        cast(refreshstatus as varchar(100))               as refresh_status,
        cast(failedreason as varchar(100))                as failed_reason
    from {{source('mysql_raw_data','plaid_accounts')}}
)

select * from plaid_accounts