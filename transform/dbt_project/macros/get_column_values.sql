{% macro get_column_values(table_name, column_name) %}
  {% set query %}
    select distinct {{ column_name }}
    from {{ table_name }}
    where {{ column_name }} is not null
    order by {{ column_name }}
  {% endset %}
  
  {% set results = run_query(query) %}
  
  {% if execute %}
    {% set values = results.columns[0].values() %}
    {{ return(values) }}
  {% else %}
    {{ return([]) }}
  {% endif %}
{% endmacro %}
