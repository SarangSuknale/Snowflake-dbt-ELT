
{{ 
    config(
        materialized = 'incremental' if target.name in ['prod', 'test'] else 'view',
        incremental_strategy = 'merge',
        unique_key = 'id',
        on_schema_change = 'sync_all_columns',
        post_hook = (
                [
                    "delete from {{ this }} "
                    "where alert_sent_date < ("
                    "  select dateadd(month, -2, coalesce(max(alert_sent_date), to_date('2020-01-01')))"
                    "  from {{ this }}"
                    ")"
                ]
                if target.name == 'test' else []
            )
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

     {% if is_incremental() and target.name == 'prod' %}
        where alert_sent_date >= (
            select dateadd('day',-15, max(alert_sent_date))
            from {{ this }}
        )
      {% elif is_incremental() and target.name == 'test' %}
        where alert_sent_date >= (
            select dateadd('day', -90, max(alert_sent_date))
            from {{ this }}
        )
    {% endif %}
)

select * from fct_alerts 