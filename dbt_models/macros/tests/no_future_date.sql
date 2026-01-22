
{% test no_future_date(model, column_name) %}

{{ config(enabled=false) }}

select 
      *
from {{model}}
where {{column_name}} > current_date()

{% endtest %}