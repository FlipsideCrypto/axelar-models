{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_rest_api(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'blocks_v2', 'sql_limit', {{var('sql_limit','100000')}}, 'producer_batch_size', {{var('producer_batch_size','100000')}}, 'worker_batch_size', {{var('worker_batch_size','50000')}}))",
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
    1000
)
SELECT
    ROUND(
        block_number,
        -3
    ) AS partition_key,
    {{ target.database }}.live.udf_api(
        'POST',
        '{service}/{Authentication}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),
        OBJECT_CONSTRUCT(
            'id',
            block_number,
            'jsonrpc',
            '2.0',
            'method',
            'block',
            'params',
            ARRAY_CONSTRUCT(
                block_number :: STRING
            )
        ),
        'vault/stg/axelar/node/mainnet'
    ) AS request
FROM
    blocks
ORDER BY
    block_number
