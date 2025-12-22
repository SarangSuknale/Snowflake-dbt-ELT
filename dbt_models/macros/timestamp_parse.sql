{% macro convert_timestamp(col) %}

case
    when regexp_like({{col}}, '^\\d{2}[-/]\\d{2}[-/]\\d{4} \\d{1,2}:\\d{1,2}$')
    then try_to_timestamp({{col}}, 'DD/MM/YYYY HH24:MI')
    when regexp_like({{col}}, '^\\d{4}[-/]\\d{2}[-/]\\d{4} \\d{1,2}:\\d{1,2}:\\d{1,2}$')
    then try_to_timestamp({{col}}, 'YYYY-MM-DD HH24:MI:SS')
    else null
    end

{% endmacro %}


