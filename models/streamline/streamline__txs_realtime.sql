{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_get_txs(object_construct('sql_source', '{{this.identifier}}'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("_block_lookback") }}
)
SELECT
    A.block_number,
    A.tx_count
FROM
    {{ ref("streamline__blocks_history") }} A
    LEFT JOIN {{ ref("streamline__txs_history") }}
    b
    ON A.block_number = b.block_number
WHERE
    A.tx_count IS NOT NULL
    AND A.tx_count > 0
    AND b.block_number IS NULL
    AND A.block_number >= (
        SELECT
            block_number
        FROM
            last_3_days
    )
ORDER BY
    1 ASC
