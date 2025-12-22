{% macro convert_date(col) %}

case
    when regexp_like({{col}}, '^\\d{2}[-/]\\d{2}[-/]\\d{4}$')
    then try_to_date({{col}}, 'DD/MM/YYYY')
    else null
    end

{% endmacro %}