
with fct_acc_balance as (
    select
          account_id,
          current_balance,
          available_balance,
          available_credit,
          balance_diffrence,
          currency,
          balance_date,
          days_since_balance_check,
          credit_utilization_ratio,
          current_balance < 0 as is_overdrawn,
          credit_utilization_ratio > 0.8 as is_high_utilization
    from {{ref('int_accounts')}}
)

select * from fct_acc_balance