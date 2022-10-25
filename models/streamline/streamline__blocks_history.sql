{{ config(
    materialized = 'view'
) }}

SELECT DISTINCT
    block_number AS block_id
FROM 
     {{ source(
    'bronze',
    'blocks'
  ) }} 
WHERE value:data:error IS NULL