{% macro create_aws_axelar_api() %}
    {{ log(
        "Creating integration for target:" ~ target
    ) }}

    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_axelar_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::924682671219:role/axelar-api-prod-rolesnowflakeudfsAF733095-T68fPXKYBRAj' api_allowed_prefixes = (
            'https://sfl36j9j2c.execute-api.us-east-1.amazonaws.com/prod/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
        {% elif target.name == "dev" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_axelar_api_dev api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::704693948482:role/axelar-api-stg-rolesnowflakeudfsAF733095-lzMHfltJRpaw' api_allowed_prefixes = (
            'https://5v36zckfli.execute-api.us-east-1.amazonaws.com/stg/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
