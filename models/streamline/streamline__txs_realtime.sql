{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_get_txs(object_construct('sql_source', '{{this.identifier}}'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

SELECT
    block_number,
    ARRAY_SIZE(
        DATA :result :block :data :txs
    ) AS tx_count
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_blocks') }}
{% else %}
    {{ ref('bronze__streamline_FR_blocks') }}
{% endif %}
WHERE
    tx_count IS NOT NULL
    AND tx_count > 0
    AND block_number NOT IN (
        SELECT
            block_number
        FROM
            {{ ref(
                "streamline__txs_history"
            ) }}
    )
ORDER BY
    1 ASC
