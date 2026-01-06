{% macro account_status_normalize(col) %}

case
    when {{col}} = 'ACTIVE' then 'active'
    when {{col}} = 'INACTIVE' then 'inactive'
    when {{col}} = 'CLOSED' then 'inactive'
    when {{col}} = 'closed' then 'inactive'
    else {{col}}
end

{% endmacro %}