
with users_login as (
    select
          login_id,
          user_id,
          login_time,
          logout_time,
          datediff('second', login_time, logout_time) as time_spend_in_seconds,
          datediff('minute', login_time, logout_time) as time_spend_in_minutes,
          login_local_ip_address,
          login_public_ip_address,
          login_ip_address,
          login_location,
          device_type,
          browser
    from {{ref('stg_users_login')}}
)

select * from users_login