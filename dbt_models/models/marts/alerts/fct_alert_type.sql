with alert_type_performance as (
    select 
          alert_type,
          channel,
          count(id) as total_alerts,
          count_if(is_success) as total_success,
          count_if(is_success)::number(10,2) / count(id) as success_rate,
          count_if(is_failed) as total_failed,
          count_if(is_failed)::number(10,2) / count(id) as failed_rate,
          count_if(is_pending) as total_pending,
          count_if(is_pending)::number(10,2) / count(id) as pending_rate
    from {{ref('int_alerts')}}
    group by alert_type, channel
)

select * from alert_type_performance