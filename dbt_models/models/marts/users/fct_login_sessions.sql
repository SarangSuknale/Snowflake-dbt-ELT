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
)

select * from user_login