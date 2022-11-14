{{ config(
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_id, -3)",
) }}

SELECT DISTINCT
    block_number AS block_id
FROM 
     {{ source(
    'bronze_streamline',
    'txs_details'
  ) }} 