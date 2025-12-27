with src1 as (
    select
          transaction_id,
          acccount_id,
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
          updated_on
    from {{ref('stg_finicity_transactions')}}
)

select * from src1