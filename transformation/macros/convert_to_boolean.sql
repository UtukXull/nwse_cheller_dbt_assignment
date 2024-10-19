{% macro convert_to_boolean(field, true_value='1', false_value='0') %}
  {# Handle true/false values (dafault to integers 1/0, for other types assign the values to the value variables) #}
CASE
  WHEN LOWER({{ field }}::text) = LOWER('{{ true_value }}') THEN TRUE
  WHEN LOWER({{ field }}::text) = LOWER('{{ false_value }}') THEN FALSE
  ELSE NULL
END :: BOOLEAN
{% endmacro %}
