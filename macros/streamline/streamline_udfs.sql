{% macro create_udf_bulk_get_blocks() %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_bulk_get_blocks() returns variant api_integration = aws_axelar_api AS {% if target.name == "prod" %}
        'https://69qcbfajak.execute-api.us-east-1.amazonaws.com/dev/bulk_get_blocks'
    {% else %}
        'https://69qcbfajak.execute-api.us-east-1.amazonaws.com/dev/bulk_get_blocks'
    {%- endif %};
{% endmacro %}

