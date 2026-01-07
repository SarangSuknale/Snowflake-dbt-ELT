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
)

select * from fct_alerts