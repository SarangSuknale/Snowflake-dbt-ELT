{% snapshot users_snap %}

{{
    config(
        target_schema = 'snapshots',
        unique_key = 'user_id',
        strategy = 'check',
        check_cols=['email','is_active','mobile_number','membership_plan','membership_status']
    )
}}

    select
          user_id,
          email,
          is_active,
          mobile_number,
          membership_plan,
          membership_status
    from {{ref('int_users')}}

{% endsnapshot %}