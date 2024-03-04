{{ config(
  materialized = 'incremental',
  full_refresh = false,
  tags = ['axelscan'],
  enabled = false
) }}

WITH dta AS (

  SELECT
    {{ target.database }}.live.udf_api(
      'GET',
      'https://api.axelarscan.io',{},{ 'method': 'getAssets' }
    ) AS DATA,
    SYSDATE() AS _inserted_timestamp
)
SELECT
  *
FROM
  dta,
  lateal flatt(
    DATA :data
  )
FROM
  dta
