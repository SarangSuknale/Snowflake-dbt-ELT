
with strip_customers as (
    select
          customer_id,
          user_id,
          email,
          name,
          created_date,
          days_since_created,
          months_since_created
    from {{ref('int_stripe_customers')}}
)

select * from strip_customers