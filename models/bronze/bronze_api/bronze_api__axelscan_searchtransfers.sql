{{ config(
  materialized = 'incremental',
  full_refresh = false
) }}

WITH ids_to_pull AS (

  SELECT
    _id
  FROM
    {{ source(
      'crosschain_silver',
      'number_sequence'
    ) }}
  WHERE
    _id <= (
      SELECT
        max_id
      FROM
        {{ ref('bronze_api__axelscan_max_ids') }}
      WHERE
        method = 'transfers'
    )

{% if is_incremental() %}
AND _id >(
  SELECT
    MAX(max_id)
  FROM
    {{ this }}
)
{% endif %}
ORDER BY
  _id
LIMIT
  100
), max_id AS (
  SELECT
    MIN(_id) AS min_id,
    MAX(_id) AS max_id,
    min_id || '-' || max_id AS id_range
  FROM
    ids_to_pull
),
calls AS (
  SELECT
    min_id,
    max_id,
    max_id - min_id + 1 AS num_ids,
    {{ target.database }}.live.udf_api(
      'GET',
      'https://api.axelarscan.io',{},{ 'method': 'searchTransfers',
      'from': min_id,
      'size': num_ids }
    ) :data :data AS DATA
  FROM
    max_id
)
SELECT
  min_id,
  max_id,
  num_ids,
  VALUE :id :: STRING AS id,
  VALUE AS DATA,
  SYSDATE() _inserted_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  calls,
  LATERAL FLATTEN(DATA)
