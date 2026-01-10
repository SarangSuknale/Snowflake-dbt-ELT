with users as (
    select
          user_id,
          full_name,
          email,
          gender,
          {{convert_date('date_of_birth')}}          as date_of_birth,
          is_active,
          country,
          state,
          city,
          street,
          cast(postal_code as varchar)               as postal_code,
          phone_number                               as mobile_number,
          {{convert_timestamp('sign_up_timestamp')}} as signup_timestamp,
          {{convert_timestamp('trail_start')}}       as trial_start,
          {{convert_timestamp('trail_end')}}         as trial_end,
          {{convert_timestamp('membership_start')}}  as membership_start,
          {{convert_timestamp('membership_end')}}    as membership_end,
          membership_plan,
          email_alerts,
          text_alerts,
          is_verified,
          is_email_verified
    from {{source('mysql_raw_data','users')}}
)

select * from users