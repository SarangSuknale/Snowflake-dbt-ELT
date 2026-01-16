
{{
    config(
        materialized = 'incremental' if target.name == 'prod' else 'view',
        incremental_strategy = 'merge',
        unique_key = 'id',
        on_schema_change='sync_all_columns'
    )
}}

with fct_alerts as (
    select
          id,
          user_id,
          channel,
          alert_name,
          alert_type,
          alert_sent_frequency,
          alert_sent_date,
          status,
          is_success,
          is_failed,
          is_pending
    from {{ref('int_alerts')}}

    {% if is_incremental() %}
    where alert_sent_date >= dateadd('day', -5, max(alert_sent_date))
    {% endif %}
)

select * from fct_alerts 