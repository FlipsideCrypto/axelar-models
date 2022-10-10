{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_get_txs()",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

SELECT 
    TO_NUMBER(TO_CHAR(SEQ4() + 1)) AS block_number
FROM
    TABLE(GENERATOR(rowcount => 4249236))

EXCEPT
SELECT
    block_id
FROM
    {{ ref(
        "streamline__txs_history"
    ) }}
ORDER BY 1 ASC
