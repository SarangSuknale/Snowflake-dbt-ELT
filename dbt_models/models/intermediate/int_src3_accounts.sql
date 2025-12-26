with src3 as (
    select
          account_id,
          user_id,
          concat('XXXX',right(account_number, 4)) as account_number_last4,
          account_type,
          account_status,
          current_balance,
          available_balance,
          (current_balance-available_balance) as balance_diffrence,
          (current_balance/nullif(current_balance + available_balance,0))::number(10,4) as credit_utilization_ratio,
          currency,
          balance_date,
          datediff('day', balance_date, current_date()) as days_since_balance_check,
          aggregation_source,
          datediff('day', last_updated, current_date()) as days_since_aggregation_success,
          case
              when last_updated < dateadd(day, -7, current_date())
              then True
              else False
          end as is_data_stale,
          created_date,
          datediff('day', created_date, current_date()) as account_age_in_days,
          datediff('month', created_date, current_date()) as account_age_in_months,
          last_updated
    from {{ref('stg_yodlee_accounts')}}
)

select * from src3