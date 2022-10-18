{% macro coalesce_prettify_timestamp(timestamp_column) %}
  DATE_TRUNC('MINUTE', CONVERT_TIMEZONE('America/Chicago', {{ timestamp_column }})::TIMESTAMP_NTZ)
{% endmacro %}