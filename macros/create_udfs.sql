{% macro create_udfs() %}
    {% set sql %}
    CREATE schema if NOT EXISTS silver;
{{ create_js_hex_to_int() }};
{{ create_udtf_get_base_table(
        schema = "streamline"
    ) }}

    {% endset %}
    {% do run_query(sql) %}
    {% set sql %}
    {{ create_udf_rest_api() }}

    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}
