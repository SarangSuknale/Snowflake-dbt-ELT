
with users_txt as (
    select
          user_id,
          count(transaction_id) as total_transactions,
          count_if(transaction_status in ('posted', 'reconciled') or not is_pending) as total_posted_txt,
          count_if(transaction_status='pending' or is_pending) as total_pending_txt,
          sum(case when amount < 0 then abs(amount) else 0 end) as total_spend,
          sum(case when amount > 0 then abs(amount) else 0 end) as total_income,
          avg(abs(amount))::number(10,2) as avg_txt_amount,
          sum(case when is_recurring and amount < 0 then abs(amount) else 0 end) as recurring_spend
    from {{ref('int_transactions')}}
    group by user_id
)

select * from users_txt