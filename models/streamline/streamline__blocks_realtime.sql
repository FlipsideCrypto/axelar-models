{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_get_blocks()",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

SELECT 
    TO_NUMBER(TO_CHAR(SEQ4() + 1)) AS block_number
FROM
    TABLE(GENERATOR(rowcount => 4000000))

EXCEPT
SELECT
    block_id
FROM
    {{ ref(
        "streamline__blocks_history"
    ) }}
ORDER BY 1 ASC
