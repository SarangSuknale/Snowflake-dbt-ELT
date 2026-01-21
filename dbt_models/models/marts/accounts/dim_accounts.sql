
with dim_accounts as (
    select
          account_id,
          user_id,
          account_number_last4,
          account_type,
          account_subtype,
          account_status,
          account_src,
          aggregation_source,
          created_date,
          account_age_in_days,
          account_age_in_months,
          last_updated,
          last_refreshed,
          days_since_last_refreshed,
          last_refresh_attempt,
          days_since_last_refresh_attempt,
          is_data_stale,
          case
              when days_since_last_refreshed <= 2 then 'fresh'
              when days_since_last_refreshed <= 7 then 'aging'
              else 'stale'
          end as freshness_bucket
    from {{ref('int_accounts')}}
) 

select * from dim_accounts