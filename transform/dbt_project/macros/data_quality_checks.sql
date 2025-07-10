{% macro test_data_quality(table_name, columns_to_check) %}
  {% set quality_tests = [] %}
  
  {% for column in columns_to_check %}
    {% set test_query %}
      select
        '{{ column.name }}' as column_name,
        'null_count' as test_type,
        count(*) as total_rows,
        count(case when {{ column.name }} is null then 1 end) as null_count,
        round(
          cast(count(case when {{ column.name }} is null then 1 end) as decimal) / 
          cast(count(*) as decimal) * 100, 2
        ) as null_percentage
      from {{ table_name }}
      
      {% if column.get('expected_values') %}
      union all
      select
        '{{ column.name }}' as column_name,
        'unexpected_values' as test_type,
        count(*) as total_rows,
        count(case when {{ column.name }} not in {{ column.expected_values }} then 1 end) as unexpected_count,
        round(
          cast(count(case when {{ column.name }} not in {{ column.expected_values }} then 1 end) as decimal) / 
          cast(count(*) as decimal) * 100, 2
        ) as unexpected_percentage
      from {{ table_name }}
      where {{ column.name }} is not null
      {% endif %}
    {% endset %}
    
    {% do quality_tests.append(test_query) %}
  {% endfor %}
  
  {% set final_query %}
    {% for test in quality_tests %}
      {{ test }}
      {% if not loop.last %} union all {% endif %}
    {% endfor %}
  {% endset %}
  
  {{ return(final_query) }}
{% endmacro %}

{% macro generate_schema_name(custom_schema_name, node) %}
  {% if custom_schema_name is none %}
    {{ target.schema }}
  {% else %}
    {{ target.schema }}_{{ custom_schema_name | trim }}
  {% endif %}
{% endmacro %}
