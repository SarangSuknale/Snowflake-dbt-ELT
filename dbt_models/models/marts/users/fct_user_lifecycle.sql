with user_life as (
    select
          user_id,
          months_since_signup,
          case
              when months_since_signup <= 1 then '0-1 month'
              when months_since_signup <=6 then '1-6 months'
              when months_since_signup <=12 then '6-12 months'
              else '12+ months'
          end as user_tenure_bucket,
          months_since_signup < 1 as is_new_user,
          months_since_signup >= 12 is_long_term_user,
          is_active as is_active_user
    from {{ref('int_users')}}
)

select * from user_life