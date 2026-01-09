with src1 as (
    select 
          account_id,
          user_id,
          concat('XXXX', right(account_number, 4)) as account_number_last4,
          {{account_type_normalization('account_type')}} as account_type,
          account_subtype,
          account_status,
          balance,
          available_balance,
          (balance-available_balance) as balance_diffrence,
          ifnull(available_credit, 0) as available_credit,
          (balance/nullif(balance + available_credit, 0))::number(10,4) as credit_utilization_ratio,
          currency,
          balance_date,
          datediff('day', balance_date, current_date()) as days_since_balance_check,
          aggregation_success_date,
          datediff('day', aggregation_success_date, current_date()) as days_since_aggregation_success,
          case 
              when aggregation_success_date < dateadd(day, -30, current_date())
              then True
              else False
          end as is_data_stale,
          aggregation_attempt_date,
          datediff('day', aggregation_attempt_date, current_date()) as days_since_aggregation_attempt,
          created_date,
          datediff('day', created_date, current_date()) as account_age_in_days,
          datediff('month', created_date, current_date()) as account_age_in_months,
          last_updated,
          'source 1' as account_src
    from {{ref('stg_finicity_accounts')}}
)

select * from src1