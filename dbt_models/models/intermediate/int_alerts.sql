with email_alerts as (
    select
          id,
          user_id,
          email as contact,
          'email' as channel,
          alert_type,
          alert_name,
          alert_sent_date,
          alert_sent_frequency,
          message_id,
          mail_subject as message_subject,
          mail_body as message_body,
          status
    from {{ref('stg_email_alerts')}}
),

text_alerts as (
    select 
          id,
          user_id,
          mobile_number as contact,
          'text' as channel,
          alert_type,
          alert_name,
          alert_sent_date,
          alert_sent_frequency,
          message_id,
          null as message_subject,
          message_body,
          status
    from {{ref('stg_text_alerts')}}
)

select 
      *,
      status = 'delivered' or status = 'bounced' as is_success,
      status = 'queued' as is_pending,
      status = 'failed' as is_failed

from email_alerts

union all

select
      *,
      status = 'delivered' as is_success,
      status = 'pending' as is_pending,
      status = 'failed' as is_failed
from text_alerts
