with user_logins as (
    select
          id as login_id,
          user_id,
          login_time,
          logout_time,
          time_spend as time_spend_in_seconds,
          login_local_ip_address,
          login_public_ip_address,
          login_ip_address,
          login_location,
          device_type,
          browser
    from {{source('mysql_raw_data','user_logins')}}
),

deduplicate_logins as (
        {{
            dbt_utils.deduplicate(
                relation='user_logins',
                partition_by='login_id, user_id',
                order_by='login_time desc',
            )
        }}
)

select * from user_logins