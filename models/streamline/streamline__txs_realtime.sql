{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_get_txs()",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

SELECT
    block_number,
    ARRAY_SIZE(
        DATA :result :block :data :txs
    ) AS tx_count
FROM
    {{ source(
        'bronze',
        'blocks'
    ) }}
WHERE
    tx_count IS NOT NULL
    AND tx_count > 0
    AND block_number NOT IN (
        SELECT
            block_id
        FROM
            {{ ref(
                "streamline__txs_history"
            ) }}
    )
ORDER BY
    1 ASC
