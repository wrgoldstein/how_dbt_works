{% macro blank_to_null(column) %}
case
  when trim({{ column }})  = '' then null
  else {{ column }}
end{% endmacro %}
