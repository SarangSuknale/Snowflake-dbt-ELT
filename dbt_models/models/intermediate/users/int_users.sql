
with users as (
    select
          user_id,
          full_name,
          email,
          gender,
          date_of_birth,
          datediff('year', date_of_birth, current_date()) as age,
          is_active,
          country,
          state,
          city,
          street,
          postal_code,
          mobile_number,
          signup_timestamp,
          datediff('month', signup_timestamp, current_date()) as months_since_signup,
          trail_start,
          trail_end,
          membership_start,
          membership_end,
          case
              when membership_end >= current_date() then datediff('day', current_date(), membership_end)
              when membership_end <= current_date() then 0
              else null
          end as membership_remaining_days,
          membership_plan,
          case
              when current_date() between membership_start and membership_end then 'active'
              when current_date() between trail_start and trail_end then 'trail'
              when membership_end < current_date() or (membership_end is null and trail_end < current_date()) then 'cancelled'
              else null
          end as membership_status,
          email_alerts,
          text_alerts,
          is_verified,
          is_email_verified,
          case
              when membership_end > current_date() then 'False'
              when membership_end < current_date() or (membership_end is null and trail_end < current_date()) then 'True'
              else null
           end as is_churn
    from {{ref('stg_users')}}
)

select * from users