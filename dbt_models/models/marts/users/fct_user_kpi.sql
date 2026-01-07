with user_kpi as (
    select
          date_trunc('month', signup_timestamp) as months,
          count(user_id) as total_users,
          count_if(months_since_signup >= 1) as new_usres,
          count_if(membership_status='active') as paid_users,
          count_if(membership_status='trail') as trail_users,
          count_if(membership_status='cancelled') as cancelled_users 
    from {{ref('int_users')}}
    group by months
)

select * from user_kpi