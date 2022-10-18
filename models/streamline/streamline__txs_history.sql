{{ config(
    materialized = 'view'
) }}

SELECT DISTINCT
    block_number AS block_id
FROM 
     {{ source(
    'bronze_streamline',
    'txs_details'
  ) }} 