{% macro create_udf_bulk_get_blocks() %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_bulk_get_blocks(
        json variant
    ) returns variant api_integration = aws_axelar_api AS {% if target.name == "prod" %}
        'https://klo2pavknf.execute-api.us-east-1.amazonaws.com/prod/bulk_get_blocks'
    {% else %}
        'https://69qcbfajak.execute-api.us-east-1.amazonaws.com/dev/bulk_get_blocks'
    {%- endif %};
{% endmacro %}

{% macro create_udf_bulk_get_txs() %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_bulk_get_txs(
        json variant
    ) returns variant api_integration = aws_axelar_api AS {% if target.name == "prod" %}
        'https://klo2pavknf.execute-api.us-east-1.amazonaws.com/prod/bulk_get_txs'
    {% else %}
        'https://69qcbfajak.execute-api.us-east-1.amazonaws.com/dev/bulk_get_txs'
    {%- endif %};
{% endmacro %}

{% macro create_udf_bulk_get_validators() %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_bulk_get_validators(
        json variant
    ) returns variant api_integration = aws_axelar_api AS {% if target.name == "prod" %}
        'https://klo2pavknf.execute-api.us-east-1.amazonaws.com/prod/bulk_get_validators'
    {% else %}
        'https://69qcbfajak.execute-api.us-east-1.amazonaws.com/dev/bulk_get_validators'
    {%- endif %};
{% endmacro %}

{% macro create_udf_get_chainhead() %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_get_chainhead() returns variant api_integration = aws_axelar_api AS {% if target.name == "prod" %}
        'https://klo2pavknf.execute-api.us-east-1.amazonaws.com/prod/get_chainhead'
    {% else %}
        'https://69qcbfajak.execute-api.us-east-1.amazonaws.com/dev/get_chainhead'
    {%- endif %};
{% endmacro %}

{% macro create_udf_rest_api() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_rest_api(
        json OBJECT
    ) returns ARRAY api_integration = {% if target.name == "prod" %}
        aws_axelar_api AS 'https://sfl36j9j2c.execute-api.us-east-1.amazonaws.com/prod/udf_bulk_rest_api'
    {% else %}
        aws_axelar_api_dev AS 'https://5v36zckfli.execute-api.us-east-1.amazonaws.com/stg/udf_bulk_rest_api'
    {%- endif %};
{% endmacro %}
