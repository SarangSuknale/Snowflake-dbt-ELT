{% macro account_type_normalization(col) %}

case
    when {{col}} Ilike '%Saving%' then 'Saving'
    when {{col}} Ilike '%Checking%' then 'Checking'
    when {{col}} Ilike '%Loan%' then 'Loan'
    when {{col}} Ilike '%Investment%' then 'Investment'
    when {{col}} Ilike '%Credit%' then 'Credit'
    when {{col}} Ilike 'Bank' then 'Saving'
    else null
end

{% endmacro %}