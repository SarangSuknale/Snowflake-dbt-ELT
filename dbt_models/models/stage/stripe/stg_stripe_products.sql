with stripe_products as (
    select
          id          as product_id,
          object,
          name        as product_name,
          description as product_description,
          active      as is_active,
          created     as created_date,
          livemode
    from {{source('mysql_raw_data','stripe_products')}}
)

select * from stripe_products