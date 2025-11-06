{{ config(
  materialized = 'incremental',
  full_refresh = false,
  tags = ['daily']
) }}

WITH dates_to_fetch AS (
  SELECT
    A.date_day,
    DATE_PART(
      epoch_second,
      A.date_day
    ) AS from_ts,
    DATE_PART(
      epoch_second,
      DATEADD(
        DAY,
        1,
        A.date_day
      )
    ) - 1 AS to_ts
  FROM
    {{ source(
      'crosschain',
      'dim_dates'
    ) }} A
  WHERE
    A.date_day >= '2025-01-01'
    AND A.date_day < SYSDATE() :: DATE

{% if is_incremental() %}
EXCEPT
SELECT
  date_day,
  from_ts,
  to_ts
FROM
  {{ this }}
{% endif %}
),
last_three_days AS (
  SELECT
    A.date_day,
    DATE_PART(
      epoch_second,
      A.date_day
    ) AS from_ts,
    DATE_PART(
      epoch_second,
      DATEADD(
        DAY,
        1,
        A.date_day
      )
    ) - 1 AS to_ts
  FROM
    {{ source(
      'crosschain',
      'dim_dates'
    ) }} A
  WHERE
    A.date_day >= SYSDATE() :: DATE - 3
    AND A.date_day < SYSDATE() :: DATE
),
date_params AS (
  SELECT DISTINCT
    date_day,
    from_ts,
    to_ts
  FROM (
    SELECT * FROM dates_to_fetch
    UNION 
    SELECT * FROM last_three_days
  )
  ORDER BY
    1 DESC
  LIMIT
    20
)
SELECT
  date_day,
  from_ts,
  to_ts,
  {{ target.database }}.live.udf_api(
    'POST',
    'https://api.axelarscan.io/gmp/GMPStatsByChains',
    OBJECT_CONSTRUCT(
      'accept',
      'application/json',
      'content-type',
      'application/json'
    ),
    OBJECT_CONSTRUCT(
      'fromTime',
      from_ts :: NUMBER,
      'toTime',
      to_ts :: NUMBER
    )
  ) AS resp,
  SYSDATE() AS _inserted_timestamp
FROM
  date_params
WHERE
  ARRAY_SIZE(
    resp :data :source_chains
  ) > 0
