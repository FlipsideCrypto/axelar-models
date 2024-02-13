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
    {{ ref('bronze_api__axelscan_id_groups_transfers') }}
  WHERE
    date_day > '2022-10-27' {# AND min_id > 1098 #}
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
  10
), calls_1 AS (
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
      'https://api.axelarscan.io',{},{ 'method': 'searchTransfers',
      'fromTime': fromTime,
      'toTime': toTime,
      'from': min_id,
      'size': num_ids }
    ) :data :data AS DATA
  FROM
    work_to_do
  ORDER BY
    date_day,
    min_id
  LIMIT
    1 offset 0
), calls_2 AS (
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
      'https://api.axelarscan.io',{},{ 'method': 'searchTransfers',
      'fromTime': fromTime,
      'toTime': toTime,
      'from': min_id,
      'size': num_ids }
    ) :data :data AS DATA
  FROM
    work_to_do
  ORDER BY
    date_day,
    min_id
  LIMIT
    1 offset 1
), calls_3 AS (
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
      'https://api.axelarscan.io',{},{ 'method': 'searchTransfers',
      'fromTime': fromTime,
      'toTime': toTime,
      'from': min_id,
      'size': num_ids }
    ) :data :data AS DATA
  FROM
    work_to_do
  ORDER BY
    date_day,
    min_id
  LIMIT
    1 offset 2
), calls_4 AS (
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
      'https://api.axelarscan.io',{},{ 'method': 'searchTransfers',
      'fromTime': fromTime,
      'toTime': toTime,
      'from': min_id,
      'size': num_ids }
    ) :data :data AS DATA
  FROM
    work_to_do
  ORDER BY
    date_day,
    min_id
  LIMIT
    1 offset 3
), calls_5 AS (
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
      'https://api.axelarscan.io',{},{ 'method': 'searchTransfers',
      'fromTime': fromTime,
      'toTime': toTime,
      'from': min_id,
      'size': num_ids }
    ) :data :data AS DATA
  FROM
    work_to_do
  ORDER BY
    date_day,
    min_id
  LIMIT
    1 offset 4
), calls_6 AS (
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
      'https://api.axelarscan.io',{},{ 'method': 'searchTransfers',
      'fromTime': fromTime,
      'toTime': toTime,
      'from': min_id,
      'size': num_ids }
    ) :data :data AS DATA
  FROM
    work_to_do
  ORDER BY
    date_day,
    min_id
  LIMIT
    1 offset 5
), calls_7 AS (
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
      'https://api.axelarscan.io',{},{ 'method': 'searchTransfers',
      'fromTime': fromTime,
      'toTime': toTime,
      'from': min_id,
      'size': num_ids }
    ) :data :data AS DATA
  FROM
    work_to_do
  ORDER BY
    date_day,
    min_id
  LIMIT
    1 offset 6
), calls_8 AS (
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
      'https://api.axelarscan.io',{},{ 'method': 'searchTransfers',
      'fromTime': fromTime,
      'toTime': toTime,
      'from': min_id,
      'size': num_ids }
    ) :data :data AS DATA
  FROM
    work_to_do
  ORDER BY
    date_day,
    min_id
  LIMIT
    1 offset 7
), calls_9 AS (
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
      'https://api.axelarscan.io',{},{ 'method': 'searchTransfers',
      'fromTime': fromTime,
      'toTime': toTime,
      'from': min_id,
      'size': num_ids }
    ) :data :data AS DATA
  FROM
    work_to_do
  ORDER BY
    date_day,
    min_id
  LIMIT
    1 offset 8
), calls_10 AS (
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
      'https://api.axelarscan.io',{},{ 'method': 'searchTransfers',
      'fromTime': fromTime,
      'toTime': toTime,
      'from': min_id,
      'size': num_ids }
    ) :data :data AS DATA
  FROM
    work_to_do
  ORDER BY
    date_day,
    min_id
  LIMIT
    1 offset 9
), all_calls AS (
  SELECT
    *
  FROM
    calls_1
  UNION ALL
  SELECT
    *
  FROM
    calls_2
  UNION ALL
  SELECT
    *
  FROM
    calls_3
  UNION ALL
  SELECT
    *
  FROM
    calls_4
  UNION ALL
  SELECT
    *
  FROM
    calls_5
  UNION ALL
  SELECT
    *
  FROM
    calls_6
  UNION ALL
  SELECT
    *
  FROM
    calls_7
  UNION ALL
  SELECT
    *
  FROM
    calls_8
  UNION ALL
  SELECT
    *
  FROM
    calls_9
  UNION ALL
  SELECT
    *
  FROM
    calls_10
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
  all_calls,
  LATERAL FLATTEN(DATA)
