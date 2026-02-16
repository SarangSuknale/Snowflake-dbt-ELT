
{{
    config(
        materialized= 'incremental' if target.name in ['prod', 'test'] else 'view',
        incremental_strategy='merge',
        unique_key='transaction_id',
        on_schema_change='sync_all_columns',
        post_hook = ([
                "delete from {{ this }} "
                "where transaction_date < ("
                "  select dateadd("
                "    month, -1, max(transaction_date)"
                "  ) "
                "  from {{ this }}"
                ")"
              ]
                if target.name == 'test' else []
            )
    )
}}

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
     {% if is_incremental() %}
        where transaction_date >= (
            select dateadd(day, -10, max(transaction_date))
            from {{ this }}
        )
    {% endif %}
)

select * from fct_txt