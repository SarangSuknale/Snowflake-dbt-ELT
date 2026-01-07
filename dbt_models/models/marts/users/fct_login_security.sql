with login_security as (
    select
           user_id,
           count(distinct login_ip_address) as unique_ip_count,
           count(distinct login_location) as unique_location_count,
           count(distinct login_ip_address) > 5 as multiple_ip_flag,
           count(distinct login_location) > 3 as possible_account_sharing,
           count(distinct device_type) as unique_device_count,
           count(distinct browser) as unique_browser_count,
           avg(time_spend_in_minutes)::number(10,2) as avg_session_duration_minutes
    from {{ref('int_users_login')}}
    group by user_id
)

select * from login_security