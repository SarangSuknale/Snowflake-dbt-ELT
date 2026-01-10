with user_membership as (
    select
          user_id,
          trial_start,
          trial_end,
          membership_plan,
          membership_status,
          membership_start,
          membership_end,
          membership_remaining_days,
          membership_status='trail' as is_trial,
          membership_status='active' as is_paid_users
    from {{ref('int_users')}}
)

select * from user_membership