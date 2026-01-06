
with dim_users_acc as (
    select
           user_id,
           min(created_date) as first_account_added_date,
           max(created_date) as last_account_added_date,
           count(account_id) as total_accounts,
           count_if(account_status='active') as active_accounts,
           count_if(account_status='inactive') as inactive_accounts,
           count_if(account_status='error') as accounts_in_error,
           count_if(account_status='pending') as pending_accounts,
           avg(account_age_in_days)::number(10,2) as avg_account_age_in_days,
           avg(account_age_in_months)::number(10,2) as avg_account_age_in_months,
           sum(case when account_type in ('Checking', 'Saving') then current_balance else 0 end) as total_assets,
           sum(case when account_type in ('Credit', 'Loan') then current_balance else 0 end) as total_liabilities,
           (sum(case when account_type in ('Checking', 'Saving') then current_balance else 0 end)
              - sum(case when account_type in ('Credit', 'Loan') then current_balance else 0 end)) as net_worth,
            avg(credit_utilization_ratio)::number(10,2) as avg_credit_utilization_ratio 
    from {{ref('int_accounts')}}
    group by user_id
)

select * from dim_users_acc