{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_rest_api(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'blocks', 'exploded_key', '[\"result\"]', 'sql_limit', {{var('sql_limit','100000')}}, 'producer_batch_size', {{var('producer_batch_size','100000')}}, 'worker_batch_size', {{var('worker_batch_size','50000')}}, 'sm_secret_name','dev/axelar/node/mainnet'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH blocks AS (

    SELECT
        block_number
    FROM
        {{ ref("streamline__blocks") }}

{% if is_incremental() %}
EXCEPT
SELECT
    block_number
FROM
    {{ ref("streamline__complete_blocks") }}
{% endif %}
ORDER BY
    1 DESC
LIMIT
    10
)
SELECT
    block_number AS partition_key,
    OBJECT_CONSTRUCT(
        'method',
        'POST',
        'url',
        '{service}/{Authentication}',
        'headers',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),
        'params',
        PARSE_JSON('{}'),
        'data',
        OBJECT_CONSTRUCT(
            'id',
            block_number :: STRING,
            'jsonrpc',
            '2.0',
            'method',
            'block',
            'params',
            ARRAY_CONSTRUCT(
                block_number :: STRING
            ) :: STRING
        )
    ) AS request
FROM
    blocks
ORDER BY
    block_number
LIMIT
    500
