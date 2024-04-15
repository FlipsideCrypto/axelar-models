{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_rest_api(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'tx_counts_v2', 'exploded_key', '[\"result\", \"total_count\"]', 'sql_limit', {{var('sql_limit','100000')}}, 'producer_batch_size', {{var('producer_batch_size','100000')}}, 'worker_batch_size', {{var('worker_batch_size','50000')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}
-- depends_on: {{ ref('streamline__complete_tx_counts') }}
WITH blocks AS (

    SELECT
        block_number
    FROM
        {{ ref("streamline__blocks") }}
    WHERE
        block_number >= 12260000
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref("streamline__complete_tx_counts") }}
)
SELECT
    ROUND(
        block_number,
        -3
    ) AS partition_key,
    live.udf_api(
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
        ),
        'vault/prod/axelar/node/mainnet'
    ) AS request
FROM
    blocks
ORDER BY
    block_number
