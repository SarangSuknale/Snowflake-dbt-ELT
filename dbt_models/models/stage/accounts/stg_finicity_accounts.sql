with finicity_accounts as (
    select
          cast(id as varchar(100))                        as account_id,
          cast(customerid as varchar(100))                as user_id,
          cast(number as number(20,0))                    as account_number,
          cast(name as varchar(100))                      as account_name,
          cast(displayname as varchar(100))               as display_name,
          cast(accounttype as varchar(50))                as account_type,
          cast(accountsubtype as varchar(50))             as account_subtype,
          cast(institutionid as number(10,0))             as institution_id,
          cast(institutionloginid as varchar(100))        as institution_login_id,
          cast(status as varchar(50))                     as account_status,
          cast(balance as number(20,2))                   as balance,
          cast(availablebalance as number(20,2))          as available_balance,
          cast(availablecredit as number(20,2))           as available_credit,
          {{convert_timestamp('balancedate')}}            as balance_date,
          {{convert_timestamp('aggregationsuccessdate')}} as aggregation_success_date,
          {{convert_timestamp('aggregationattemptdate')}} as aggregation_attempt_date,
          {{convert_timestamp('createddate')}}            as created_date,
          cast(currency as varchar(10))                   as currency,
          cast(routingnumber as number(20))               as routing_number,
          cast(iban as varchar(100))                      as iban,
          cast(bic as varchar(100))                       as bic,
          {{convert_timestamp('last_updated')}}           as last_updated,
          cast(interestrate as number(5,2))               as interest_rate,
          cast(loanterm as varchar(100))                  as loan_term,
          {{convert_date('maturitydate')}}                as maturity_date,
          cast(originalloanamount as number(20,2))        as original_loan_amount,
          cast(currentbalance as number(20,2))            as current_balance,
          cast(paymentamount as number(20,2))             as payment_amount,
          cast(lastpaymentamount as number(20,2))         as last_payment_amount,
          {{convert_date('lastpaymentdate')}}             as last_payment_date,
          {{convert_date('nextpaymentdate')}}             as next_payment_date
    from {{ source('mysql_raw_data','finicity_accounts') }}
)

select * from finicity_accounts