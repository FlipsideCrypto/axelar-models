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

{% macro create_udf_get_chainhead() %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_get_chainhead() returns variant api_integration = aws_axelar_api AS {% if target.name == "prod" %}
        'https://klo2pavknf.execute-api.us-east-1.amazonaws.com/prod/get_chainhead'
    {% else %}
        'https://69qcbfajak.execute-api.us-east-1.amazonaws.com/dev/get_chainhead'
    {%- endif %};
{% endmacro %}
