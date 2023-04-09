{% macro run_all_stored_procedures_demo(dry_run=True) %}
    {% set sql %}
        --==============================================================================================
        -- COPY FROM HERE 🎷🎷 ===============================================
        --===============================================

        {{setup_stored_procedures_demo(            'sp_to_dbt', dry_run) }}
        {{build_and_process_stored_procedures_demo('sp_to_dbt_dev', 'sp_to_dbt_developer', 'sp_to_dbt_dev_wh', dry_run) }}
        {{teardown_stored_procedures_demo(         'sp_to_dbt', dry_run) }}
    {% endset %}

    {{ log(sql, info=True) }}

    {{ return(sql) }}
{% endmacro %}