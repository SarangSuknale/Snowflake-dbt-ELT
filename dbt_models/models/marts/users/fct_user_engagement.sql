with user_eng as (
    select
          user_id,
          count(login_id) as total_sessions,
          sum(time_spend_in_minutes) as total_time_spend_minutes,
          avg(time_spend_in_minutes)::number(10,2) as avg_session_duration_minutes,
          max(login_time) as last_login,
          datediff('day', max(login_time), current_date()) as days_since_last_login,
          max(login_time) >= dateadd(day, -30, current_date()) as active_last_30_days
    from {{ref("int_users_login")}}
    group by user_id
)

select * from user_eng