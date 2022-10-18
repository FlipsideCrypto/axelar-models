{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_get_blocks(object_construct('sql_source', '{{this.identifier}}'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

{% if execute %}
    {% set height = run_query('SELECT streamline.udf_get_chainhead()') %}
    {% set block_height = height.columns [0].values() [0] %}
{% else %}
    {% set block_height = 0 %}
{% endif %}

SELECT
    height AS block_number
FROM
    TABLE(streamline.udtf_get_base_table({{ block_height }}))
EXCEPT
SELECT
    block_id
FROM
    {{ ref(
        "streamline__blocks_history"
    ) }}
ORDER BY
    1 ASC
