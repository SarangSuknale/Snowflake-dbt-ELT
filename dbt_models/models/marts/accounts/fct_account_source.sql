
with account_src as (
    select
          account_src,
          count(account_id) as total_accounts,
          count_if(account_status='active') as active_accounts,
          count_if(account_status='inactive') as inactive_accounts,
          count_if(account_status='error') as accounts_in_error,
          count_if(account_status='pending') as pending_accounts,
          count_if(is_data_stale) as stale_accounts
    from {{ref('int_accounts')}}
    group by account_src
)

select * from account_src