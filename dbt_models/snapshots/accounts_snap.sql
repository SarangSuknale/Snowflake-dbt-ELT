{% snapshot accounts_snap %}
 
 {{
    config(
        target_schema = 'snapshots',
        unique_key = 'account_id',
        strategy = 'check',
        check_cols = ['account_status'],
        invalidate_hard_deletes = true
    )
 }}


    select 
          account_id,
          account_status
    from {{ref('int_accounts')}}

{% endsnapshot %}