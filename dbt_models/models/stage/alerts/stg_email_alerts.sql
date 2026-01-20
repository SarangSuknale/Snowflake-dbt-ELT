with email_alerts as (
    select
          id,
          user_id,
          email,
          alert_type,
          alert_name,
          date as alert_sent_date,
          frequency as alert_sent_frequency,
          message_id,
          mail_subject,
          mail_body,
          status 
    from {{source('mysql_raw_data','email_alerts')}}
)

select * from email_alerts