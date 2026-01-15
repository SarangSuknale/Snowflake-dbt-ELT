{% snapshot stripe_customers_snap %}

{{
    config(
        target_schema = 'snapshots',
        unique_key = 'customer_id',
        strategy = 'check',
        check_cols = ['customer_id', 'email']
    )
}}

    select 
          customer_id,
          email
    from {{ref('int_stripe_customers')}}

{% endsnapshot %}