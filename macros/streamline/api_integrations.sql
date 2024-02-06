{% macro create_aws_axelar_api() %}
    {{ log(
        "Creating integration for target:" ~ target
    ) }}

    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_axelar_api api_provider = aws_api_gateway api_aws_role_arn = '' api_allowed_prefixes = (
            ''
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
        {% elif target.name == "dev" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_axelar_api_dev api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::704693948482:role/axelar-api-dev-rolesnowflakeudfsAF733095-iB7eC2WbyFvY' api_allowed_prefixes = (
            'https://q8knm7tyk5.execute-api.us-east-1.amazonaws.com/dev/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
