
{{
    config(
        materialized = 'incremental' if target.name == 'prod' else 'view' ,
        incremental_strategy = 'merge',
        unique_key = ['alert_date', 'channel'],
        on_schema_change='sync_all_columns'
    )
}}

with alert_metric as (
    select
          date_trunc('day', alert_sent_date) as alert_date,
          channel,
          count(id) as total_alerts,
          count_if(is_success) as total_success,
          count_if(is_success)::number(10,2) / count(id) as success_rate,
          count_if(is_failed) as total_failed,
          count_if(is_failed)::number(10,2) / count(id) as failed_rate,
          count_if(is_pending) as total_pending,
          count_if(is_pending)::number(10,2) / count(id) as pending_rate
    from {{ref('int_alerts')}}
    {% if is_incremental() %}
    where alert_date >= dateadd('day', -2, max(alert_date))
    {% endif %}
    group by alert_date, channel
)

select * from alert_metric