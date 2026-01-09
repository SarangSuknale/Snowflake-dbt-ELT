with text_alerts as (
    select
          id,
          user_id,
          alert_type,
          alert_name,
          contact_number as mobile_number,
          date as alert_sent_date,
          frequency as alert_sent_frequency,
          message_id,
          message_body,
          status
    from {{source('mysql_raw_data','text_alerts')}}
)

select * from text_alerts