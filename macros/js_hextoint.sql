{% macro create_js_hex_to_int() %}
    CREATE FUNCTION IF NOT EXISTS {{ target.schema }}.js_hex_to_int (
        s STRING
    ) returns DOUBLE LANGUAGE javascript AS 'if (S !== null) { yourNumber = parseInt(S, 16); } return yourNumber'
{% endmacro %}
