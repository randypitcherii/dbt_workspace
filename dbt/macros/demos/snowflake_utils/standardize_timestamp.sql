{% macro standardize_timestamp(timestamp_column) %}
  CONVERT_TIMEZONE('America/New_York', {{ timestamp_column }})::TIMESTAMP_NTZ
{% endmacro %}