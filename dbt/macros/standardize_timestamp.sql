{% macro standardize_timestamp(timestamp_column) %}
  CONVERT_TIMEZONE('America/Chicago', {{ timestamp_column }})::TIMESTAMP_NTZ
{% endmacro %}