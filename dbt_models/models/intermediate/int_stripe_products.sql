
with price as (
    select
          price_id,
          product_id,
          price,
          currency,
          interval,
          is_active,
          created_date as last_price_updated,
          datediff('day', created_date, current_date()) as days_since_price_updated,
          datediff('month', created_date, current_date()) as months_since_price_updated
    from {{ref('stg_stripe_price')}}
),

products as (
    select
          product_id,
          product_name,
          product_description,
          is_active,
          created_date,
          datediff('day', created_date, current_date()) as days_since_product_created,
          datediff('month', created_date, current_date()) as months_since_product_created
    from {{ref('stg_stripe_products')}}
),

final as (
    select
          p.price_id,
          p.product_id,
          p.price as product_price,
          p.currency,
          p.interval,
          p.is_active as is_price_active,
          p.last_price_updated,
          p.days_since_price_updated,
          p.months_since_price_updated,
          pd.product_name,
          pd.product_description,
          pd.is_active as is_product_active,
          pd.created_date as product_created_date,
          pd.days_since_product_created,
          pd.months_since_product_created
    from price p
    inner join products pd 
    on p.product_id=pd.product_id
)

select * from final