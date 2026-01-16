
with src3_txt as (
    select
          transaction_id,
          account_id,
          transaction_date,
          datediff('month', transaction_date, current_date()) as transaction_age_in_months,
          amount,
          category,
          merchant,
          is_manual,
          is_physical as is_verified,
          post_date,
          last_updated,
          datediff('day', last_updated, current_date()) as days_since_updated,
          'Source 3' as txt_src
    from {{ref('stg_yodlee_transactions')}}
)

select * from src3_txt