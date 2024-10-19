{% macro transform_date_to_id(date_column, table_alias=None) %}
    {% if table_alias is not none %}
        EXTRACT(YEAR FROM {{ table_alias }}.{{ date_column }}) * 10000 + 
        EXTRACT(MONTH FROM {{ table_alias }}.{{ date_column }}) * 100 + 
        EXTRACT(DAY FROM {{ table_alias }}.{{ date_column }})
    {% else %}
        EXTRACT(YEAR FROM {{ date_column }}) * 10000 + 
        EXTRACT(MONTH FROM {{ date_column }}) * 100 + 
        EXTRACT(DAY FROM {{ date_column }})
    {% endif %}
{% endmacro %}