with dim_users as (
    select
          user_id,
          full_name,
          email,
          mobile_number,
          gender,
          date_of_birth,
          age,
          country,
          state,
          city,
          street,
          postal_code,
          is_active,
          is_verified,
          is_email_verified,
          signup_timestamp,
          months_since_signup
    from {{ref('int_users')}}
)

select * from dim_users