
{{
    config(
        materialized = 'incremental' if target.name == 'prod' else 'view',
        incremental_strategy = 'merge',
        unique_key = 'login_id',
        on_schema_change='sync_all_columns'
    )
}}

with user_login as (
    select
          login_id,
          user_id,
          login_time,
          logout_time,
          datediff('second', login_time, logout_time) as session_duration_seconds,
          datediff('minute', login_time, logout_time) as session_duration_minutes,
          login_ip_address,
          login_local_ip_address,
          login_public_ip_address,
          login_location,
          device_type,
          browser as browser_type,
          logout_time is not null as is_successfull_session
    from {{ref('int_users_login')}}
    {% if is_incremental() %}
    where login_time >= (
          select dateadd(day, -5, max(login_time)) 
          from {{ this }})
    {% endif %}
)

select * from user_login 