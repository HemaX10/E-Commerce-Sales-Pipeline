{% macro safe_integer_cast(expression) %}
    cast({{ expression }} as integer)
{% endmacro %}