{{ config(
  materialized = 'incremental',
  unique_key = 'date_day',
  full_refresh = false,
  tags = ['axelscan']
) }}

WITH dates_hist AS (

  SELECT
    A.date_day,
    DATE_PART(
      epoch_second,
      A.date_day
    ) AS fromTime,
    DATE_PART(epoch_second, DATEADD (DAY, 1, A.date_day)) -1 AS toTime
  FROM
    {{ source(
      'crosschain',
      'dim_dates'
    ) }} A

{% if is_incremental() %}
LEFT JOIN {{ this }}
b
ON A.date_day = b.date_day
WHERE
  b.date_day IS NULL
  AND A.date_day BETWEEN '2021-12-23'
  AND SYSDATE() :: DATE - 2
{% else %}
WHERE
  A.date_day BETWEEN '2021-12-23'
  AND SYSDATE() :: DATE - 2
{% endif %}
),
dates_recent AS (
  SELECT
    date_day,
    DATE_PART(
      epoch_second,
      date_day
    ) AS fromTime,
    DATE_PART(epoch_second, DATEADD (DAY, 1, date_day)) -1 AS toTime
  FROM
    {{ source(
      'crosschain',
      'dim_dates'
    ) }}
  WHERE
    date_day BETWEEN SYSDATE() :: DATE - 1
    AND SYSDATE() :: DATE
),
date_combo AS (
  SELECT
    date_day,
    fromTime,
    toTime
  FROM
    dates_hist
  UNION ALL
  SELECT
    date_day,
    fromTime,
    toTime
  FROM
    dates_recent
)
SELECT
  date_day,
  fromTime,
  toTime,
  {{ target.database }}.live.udf_api(
    'POST',
    'https://api.axelarscan.io/token/searchTransfers',{},{ 'fromTime': fromTime,
    'toTime': toTime,
    'size': 1 }
  ) :data :total AS day_count,
  SYSDATE() AS _inserted_timestamp
FROM
  date_combo
WHERE
  day_count IS NOT NULL
ORDER BY
  date_day
LIMIT
  20
