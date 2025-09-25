{{ config (
    materialized = 'view'
) }}

SELECT
    partition_key,
    DATA,
    _INSERTED_TIMESTAMP,
    id,
    metadata,
    file_name,
    _PARTITION_BY_BLOCK_ID,
    VALUE
FROM
    {{ ref('bronze__streamline_FR_transactions_v2') }}
