
with yodlee_accounts as (
    select 
          id                                     as account_id,
          user_id,
          provideraccountid                      as provider_account_id,
          accountname                            as account_name,
          accounttype                            as account_type,
          accountstatus                          as account_status,
          accountnumber                          as account_number,
          routingnumber                          as routing_number,
          iban,
          bic,
          cast(currentbalance as number(10,2))   as current_balance,
          cast(availablebalance as number(10,2)) as available_balance,
          currency,
          balancedate                            as balance_date,
          displayedname                          as displayed_name,
          holder                                 as account_holder_name,
          cast(isasset as boolean)               as is_asset,
          cast(ismanual as boolean)              as is_manual,
          aggregationsource                      as aggregation_source,
          createddate                            as created_date,
          lastupdated                            as last_updated,
          container,
          providername                           as provider_name,
          providerid                             as provider_id
    from {{source('mysql_raw_data','yodlee_accounts')}}
)

select * from yodlee_accounts