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
    date_day IN (
      '2022-02-01',
      '2022-02-02',
      '2022-02-03',
      '2022-02-04',
      '2022-02-07',
      '2022-02-08',
      '2022-02-09',
      '2022-02-10',
      '2022-02-11',
      '2022-02-12',
      '2022-02-13',
      '2022-02-14',
      '2022-02-15',
      '2022-02-16',
      '2022-02-17',
      '2022-02-18',
      '2022-02-19',
      '2022-02-20',
      '2022-02-21',
      '2022-02-22',
      '2022-02-23',
      '2022-02-24',
      '2022-02-25',
      '2022-02-26',
      '2022-02-27',
      '2022-02-28',
      '2022-03-01',
      '2022-03-02',
      '2022-03-03',
      '2022-03-04',
      '2022-03-05',
      '2022-03-06',
      '2022-03-07',
      '2022-03-08',
      '2022-03-09',
      '2022-03-10',
      '2022-03-11',
      '2022-03-12',
      '2022-03-13',
      '2022-03-14',
      '2022-03-15',
      '2022-03-16',
      '2022-03-17',
      '2022-03-18',
      '2022-03-19',
      '2022-03-20',
      '2022-03-23',
      '2022-03-28',
      '2022-04-15',
      '2022-04-16',
      '2022-04-21',
      '2022-04-24',
      '2022-04-27',
      '2022-05-11',
      '2022-05-12',
      '2022-05-13',
      '2022-05-16',
      '2022-05-28',
      '2022-09-04',
      '2022-09-06',
      '2022-09-08',
      '2022-09-09',
      '2022-09-10',
      '2022-09-11',
      '2022-09-12',
      '2022-09-14',
      '2022-09-30',
      '2022-10-28',
      '2022-11-04',
      '2022-11-05',
      '2022-11-26',
      '2022-11-29',
      '2022-11-30',
      '2022-12-02',
      '2022-12-16',
      '2022-12-19',
      '2022-12-20',
      '2023-02-02',
      '2023-02-03',
      '2023-03-21',
      '2023-04-12',
      '2023-06-09',
      '2023-06-19',
      '2023-06-26'
    )
    OR (
      date_day >= '2024-02-22'
      AND date_day < SYSDATE() :: DATE
    )
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
WHERE
  _inserted_timestamp > '2024-02-26 20:05:46.179'
{% endif %}
ORDER BY
  date_day DESC,
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
