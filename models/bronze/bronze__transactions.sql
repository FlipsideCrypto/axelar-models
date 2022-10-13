{{ config(
    materialized = 'view'
) }}

SELECT 
    value, 
    _partition_by_block_id, 
    block_number AS block_id, 
    metadata, 
    data
FROM 
     {{ source(
    'bronze',
    'txs'
  ) }} 