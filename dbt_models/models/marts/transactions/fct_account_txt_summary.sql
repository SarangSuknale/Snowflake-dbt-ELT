
with acc_txt_summary as (
    select
          account_id,
          count(transaction_id) as total_transactions,
          count_if(transaction_status in ('posted', 'reconciled') or not is_pending) as total_posted_txt,
          count_if(transaction_status='pending' or is_pending) as total_pending_txt,
          count_if(amount < 0) as debit_count,
          count_if(amount > 0) as credit_count,
          sum(case when amount < 0 then abs(amount) else 0 end) as total_debits,
          sum(case when amount > 0 then abs(amount) else 0 end) as total_credits,
          avg(abs(amount))::number(10,2) as avg_txt_amount
    from {{'int_transactions'}}
    group by account_id
)

select * from acc_txt_summary