{% snapshot stripe_customers_snap %}

{{
    config(
        target_schema = 'snapshots',
        unique_key = 'customer_id',
        strategy = 'check',
        check_cols = ['customer_id', 'email'],
        invalidate_hard_deletes = true
    )
}}

    select 
          customer_id,
          email
    from {{ref('int_stripe_customers')}}

{% endsnapshot %}