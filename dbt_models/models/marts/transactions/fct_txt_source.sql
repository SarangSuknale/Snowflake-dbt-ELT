
with txt_source as (
    select
          txt_src as transaction_source,
          count(transaction_id) as total_transactions,
          count_if(transaction_status in ('posted', 'reconciled') or not is_pending) as posted_transactions,
          count_if(transaction_status='pending' or is_pending) as pending_transactions
    from {{ref('int_transactions')}}
    group by txt_src
)

select * from txt_source