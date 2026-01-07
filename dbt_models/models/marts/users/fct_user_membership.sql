with user_membership as (
    select
          user_id,
          trail_start,
          trail_end,
          membership_plan,
          membership_status,
          membership_start,
          membership_end,
          membership_remaining_days,
          membership_status='trail' as is_trail,
          membership_status='active' as is_paid_users
    from {{ref('int_users')}}
)

select * from user_membership