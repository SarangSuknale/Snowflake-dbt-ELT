with alert_eng as (
    select
          user_id,
          count(id) as total_alerts,
          count_if(channel='email') as email_alerts,
          count_if(channel='text') as text_alerts,
          max(alert_sent_date) as last_alert_send_on,
          case
              when count_if(channel='email') > count_if(channel='text') then 'email'
              when count_if(channel='email') < count_if(channel='text') then 'text'
              else 'mixed'
          end as channel_preference
    from {{ref('int_alerts')}}
    group by user_id
)

select * from alert_eng