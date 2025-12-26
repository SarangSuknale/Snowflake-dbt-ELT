with src2 as (
        select
            account_id,
            user_id,
            concat('XXXX', right(account_number, 4)) as account_number_last4,
            account_type,
            current_balance,
            ifnull(available_balance,0) as available_balance,
            (current_balance-available_balance) as balance_diffrence,
            cast(case 
                    when (current_balance + available_balance) > 0 
                    then (current_balance / (current_balance + available_balance)) 
                    else 0 
                end as number(10,2)) as credit_utilization_pct,
            currency,
            last_refreshed,
            datediff('day', last_refreshed, current_date()) as days_since_last_refreshed,
            last_refresh_attempt,
            datediff('day', last_refresh_attempt, current_date()) as days_since_last_refresh_attempt,
            case 
                when last_refreshed < dateadd(day, -30, current_date())
                then True
                else False
            end as is_data_stale,
            datediff('day', created_date, current_date()) as account_age_in_days,
            datediff('month', created_date, current_date()) as account_age_in_months,
            aggregation_source,
            last_updated
        from {{ref('stg_plaid_accounts')}}
)

select * from src2