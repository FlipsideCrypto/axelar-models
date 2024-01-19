{{ config(
  materialized = 'table'
) }}

SELECT
  'transfers' AS method,
  {{ target.database }}.live.udf_api(
    'GET',
    'https://api.axelarscan.io',{},{ 'method': 'searchTransfers',
    'from': 1,
    'size': 10 }
  ) :data :total AS max_id,
  SYSDATE() AS _inserted_timestamp
UNION ALL
SELECT
  'gmp' AS method,
  {{ target.database }}.live.udf_api(
    'GET',
    'https://api.gmp.axelarscan.io',{},{ 'method': 'searchGMP',
    'from': 1,
    'size': 1 }
  ) :data :total AS max_id,
  SYSDATE() AS _inserted_timestamp
