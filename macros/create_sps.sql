{% macro create_sps() %}
    {% if target.database == 'AXELAR' %}
        CREATE schema IF NOT EXISTS _internal;
CREATE schema IF NOT EXISTS _datashare;
{{ sp_create_prod_clone('_internal') }};
    {% endif %}
{% endmacro %}
