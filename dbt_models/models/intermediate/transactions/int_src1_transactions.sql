
with src1 as (
    select
          transaction_id,
          account_id,
          user_id,
          transaction_date,
          datediff('month', transaction_date, current_date()) as transaction_age_in_months,
          amount,
          currency,
          category,
          merchant,
          is_manually_added,
          is_recurring,
          is_verified,
          post_date,
          transaction_status,
          transaction_modified,
          updated_on,
          'source 1' as txt_src
    from {{ref('stg_finicity_transactions')}}
)

select * from src1