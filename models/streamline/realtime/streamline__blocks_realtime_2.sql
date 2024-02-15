{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_rest_api(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'blocks_test_queue', 'sql_limit', {{var('sql_limit','100000')}}, 'producer_batch_size', {{var('producer_batch_size','100000')}}, 'worker_batch_size', {{var('worker_batch_size','50000')}}, 'sm_secret_name','dev/axelar/node/mainnet'))",
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
    ARRAY_CONSTRUCT(
        block_number :: STRING,
        ARRAY_CONSTRUCT(
            'POST',
            '{service}/{Authentication}',
            PARSE_JSON('{}'),
            PARSE_JSON('{}'),
            OBJECT_CONSTRUCT(
                'method',
                'block',
                'params',
                ARRAY_CONSTRUCT(
                    block_number :: STRING
                ),
                'id',
                block_number,
                'jsonrpc',
                '2.0'
            )
        )
    ) AS request
FROM
    blocks
ORDER BY
    block_number
