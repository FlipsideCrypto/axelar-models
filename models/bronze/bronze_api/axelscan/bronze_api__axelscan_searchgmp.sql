{{ config(
  materialized = 'incremental',
  full_refresh = false,
  cluster_by = ['_inserted_timestamp::DATE'],
  tags = ['axelscan','axelscan_search']
) }}

WITH max_id AS (

  SELECT
    date_day,
    fromTime,
    toTime,
    group_id,
    min_id,
    max_id,
    id_range
  FROM
    {{ ref('bronze_api__axelscan_id_groups_gmp') }}
  WHERE
    date_day > '2023-02-24'
),
work_to_do AS (
  SELECT
    date_day,
    fromTime,
    toTime,
    group_id,
    min_id,
    max_id
  FROM
    max_id

{% if is_incremental() %}
EXCEPT
SELECT
  date_day,
  fromTime,
  toTime,
  group_id,
  min_id,
  max_id
FROM
  {{ this }}
{% endif %}
ORDER BY
  date_day,
  group_id
LIMIT
  2
), calls AS (
  SELECT
    date_day,
    fromTime,
    toTime,
    group_id,
    min_id,
    max_id,
    max_id - min_id + 1 AS num_ids,
    {{ target.database }}.live.udf_api(
      'GET',
      'https://api.gmp.axelarscan.io',{},{ 'method': 'searchGMP',
      'fromTime': fromTime,
      'toTime': toTime,
      'from': min_id,
      'size': num_ids }
    ) :data :data AS DATA
  FROM
    work_to_do
)
SELECT
  date_day,
  fromTime,
  toTime,
  group_id,
  num_ids,
  min_id,
  max_id,
  VALUE :id :: STRING AS id,
  VALUE AS DATA,
  SYSDATE() _inserted_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  calls,
  LATERAL FLATTEN(DATA)
