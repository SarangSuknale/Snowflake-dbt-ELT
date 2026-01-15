{% test range_between(model, column_name, min_value, max_value) %}

select 
      *
from {{model}}
where {{column_name}} <= {{min_value}}
  and {{column_name}} >= {{max_value}}

{% endtest %}