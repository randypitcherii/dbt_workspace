{% macro get_environment_id() %}
    {% if 'default' == target.name %}
        {{ return('D') }}
    {% elif target.name.startswith('dev_') %}
        {{ return('D') }}
    {% elif target.name.startswith('sit_') %}
        {{ return('S') }}
    {% elif target.name.startswith('mod_') %}
        {{ return('M') }}
    {% else %}
        {{ exceptions.raise_compiler_error("Unrecognized target name. Got: " ~ target.name) }}
    {% endif %}
{% endmacro %}

{% macro use_role() %}
    use role RL_{{this.database}}_DEPLOYER_{{get_environment_id()}};
{% endmacro %}