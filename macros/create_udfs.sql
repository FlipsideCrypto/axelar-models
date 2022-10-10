{% macro create_udfs() %}
    {% set sql %}
    {{ create_udf_bulk_get_blocks() }}
    {{ create_udf_bulk_get_txs() }}
    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}
