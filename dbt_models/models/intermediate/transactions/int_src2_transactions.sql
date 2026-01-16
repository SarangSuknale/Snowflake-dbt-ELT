
{{
    config(
        materialized='ephemeral'
    )
}}

with src2_txt as (
    select
          transaction_id,
          account_id,
          transaction_date,
          datediff('month', transaction_date, current_date()) as transaction_age_in_months,
          amount,
          currency,
          authorized_date,
          regexp_replace(category, '[^[:alpha:]& ]', '') as category,
          merchant_name as merchant,
          payment_channel,
          is_pending,
          'Source 2' as txt_src
    from {{ref('stg_plaid_transactions')}}
)

select * from src2_txt