{% snapshot stripe_payment_snap %}

{{
    config(
        target_schema = 'snapshots',
        unique_key = 'payment_id',
        strategy = 'check',
        check_cols = ['status', 'payment_method_type', 'card_brand']
    )
}}

    select
          payment_id,
          status,
          payment_method_type,
          card_brand
    from {{ref('int_stripe_payment')}}

{% endsnapshot %}