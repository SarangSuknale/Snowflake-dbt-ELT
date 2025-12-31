with txt as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref('int_src1_transactions'),
                ref('int_src2_transactions'),
                ref('int_src3_transactions')
            ]
        )
    }}
)

select 
       transaction_id,
       account_id,
       user_id,
       transaction_date,
       transaction_age_in_months,
       amount,
       currency,
       category,
       merchant,
       coalesce(is_manually_added, is_manual) as is_manual,
       is_recurring,
       is_verified,
       coalesce(post_date, authorized_date) as post_date,
       transaction_status,
       coalesce(updated_on, last_updated, null) as last_updated,
       payment_channel,
       is_pending,
       txt_src
from txt