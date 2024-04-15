{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_rest_api(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'txs_v2', 'exploded_key', '[\"result\", \"txs\"]', 'sql_limit', {{var('sql_limit','100000')}}, 'producer_batch_size', {{var('producer_batch_size','100000')}}, 'worker_batch_size', {{var('worker_batch_size','50000')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}
-- depends_on: {{ ref('streamline__complete_transactions') }}
-- depends_on: {{ ref('streamline__complete_tx_counts') }}
WITH blocks AS (

    SELECT
        A.block_number,
        tx_count
    FROM
        {{ ref("streamline__complete_tx_counts") }} A
        LEFT JOIN {{ ref("streamline__complete_transactions") }}
        b
        ON A.block_number = b.block_number
    WHERE
        b.block_number IS NULL
),
numbers AS (
    -- Recursive CTE to generate numbers. We'll use the maximum txcount value to limit our recursion.
    SELECT
        1 AS n
    UNION ALL
    SELECT
        n + 1
    FROM
        numbers
    WHERE
        n < (
            SELECT
                CEIL(MAX(tx_count) / 100.0)
            FROM
                blocks)
        ),
        blocks_with_page_numbers AS (
            SELECT
                tt.block_number AS block_number,
                n.n AS page_number
            FROM
                blocks tt
                JOIN numbers n
                ON n.n <= CASE
                    WHEN tt.tx_count % 100 = 0 THEN tt.tx_count / 100
                    ELSE FLOOR(
                        tt.tx_count / 100
                    ) + 1
                END
            WHERE
                tt.tx_count > 0
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
                    page_number :: STRING,
                    '100',
                    'asc',
                    FALSE
                )
            ),
            'vault/prod/axelar/node/mainnet'
        ) AS request
    FROM
        blocks_with_page_numbers
    ORDER BY
        block_number
