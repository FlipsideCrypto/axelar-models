{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_rest_api(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'tx_counts', 'sql_limit', {{var('sql_limit','100000')}}, 'producer_batch_size', {{var('producer_batch_size','100000')}}, 'worker_batch_size', {{var('worker_batch_size','50000')}} ,'secret','vault/stg/axelar/node/mainnet' ))",
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
    100
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
        'data',
        OBJECT_CONSTRUCT(
            'id',
            block_number,
            'jsonrpc',
            '2.0',
            'method',
            'tx_search',
            'params',
            ARRAY_CONSTRUCT(
                'tx.height=' || block_number :: STRING,
                TRUE,
                '1',
                '1',
                'asc',
                FALSE
            )
        ) :: STRING
    ) AS request
FROM
    blocks
ORDER BY
    block_number
