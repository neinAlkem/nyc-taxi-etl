{% macro truncate_table(table_name) %}
  {%- set sql %}
    TRUNCATE TABLE {{ table_name }}
  {%- endset %}

  {% do run_query(sql) %}
{% endmacro %}
