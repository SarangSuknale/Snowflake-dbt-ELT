
with accounts as (
    {{
    dbt_utils.union_relations(
        relations=[
            ref('int_src1_accounts'),
            ref('int_src2_accounts'),
            ref('int_src3_accounts')
        ],

        source_column_name='source_system'    
    )
}}
)

select 
       account_id,
       user_id,
       account_number_last4,
       account_type,
       account_subtype,
       account_status,
       coalesce(balance, current_balance) as current_balance,
       available_balance,
       balance_diffrence,
       available_credit,
       credit_utilization_ratio,
       currency,
       balance_date,
       days_since_balance_check,
       coalesce(aggregation_success_date, last_refreshed) as last_refreshed,
       coalesce(days_since_aggregation_success, days_since_last_refreshed) as days_since_last_refreshed,
       is_data_stale,
       coalesce(aggregation_attempt_date, last_refresh_attempt) as last_refresh_attempt,
       coalesce(days_since_aggregation_attempt, days_since_last_refresh_attempt) as days_since_last_refresh_attempt,
       created_date,
       account_age_in_days,
       account_age_in_months,
       last_updated,
       account_src,
       aggregation_source
from accounts