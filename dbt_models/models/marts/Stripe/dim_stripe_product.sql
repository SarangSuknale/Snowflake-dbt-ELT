
with product_price as (
    select
          product_id,
          price_id,
          product_price,
          currency,
          interval,
          is_price_active,
          last_price_updated,
          days_since_price_updated,
          months_since_price_updated,
          product_name,
          product_description,
          is_product_active,
          product_created_date,
          days_since_product_created,
          months_since_product_created
    from {{ref('int_stripe_products')}}
)

select * from product_price