
with fct_txt as (
    select
          transaction_id,
          account_id,
          user_id,
          transaction_date,
          post_date,
          amount,
          abs(amount) as abs_amount,
          currency,
          category,
          merchant,
          payment_channel,
          transaction_status,
          amount < 0 as is_debit,
          amount > 0 as is_credit,
          is_pending,
          is_verified,
          is_manual,
          is_recurring,
          txt_src,
          last_updated
    from {{ref('int_transactions')}}
)

select * from fct_txt