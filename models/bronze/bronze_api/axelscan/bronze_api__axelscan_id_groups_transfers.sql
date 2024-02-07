{{ config(
  materialized = 'incremental',
  unique_key = 'date_day',
  incremental_strategy = 'delete+insert',
  tags = ['axelscan']
) }}

WITH ids_days AS (

  SELECT
    date_day,
    fromTime,
    toTime,
    _id -1 AS _id,
    _inserted_timestamp
  FROM
    (
      SELECT
        _id
      FROM
        {{ source(
          'crosschain_silver',
          'number_sequence'
        ) }}
      WHERE
        _id <= 1000000
    ) A
    CROSS JOIN {{ ref('bronze_api__axelscan_day_counts_transfers') }}
    b
  WHERE
    A._id <= b.day_count

{% if is_incremental() %}
AND _inserted_timestamp >= (
  SELECT
    MAX(
      _inserted_timestamp
    )
  FROM
    {{ this }}
)
{% endif %}
),
groupings AS (
  SELECT
    date_day,
    fromTime,
    toTime,
    _id,
    _inserted_timestamp,
    --this logic breaks the ids into sets of 20
    conditional_true_event(
      CASE
        WHEN MOD(
          _id,
          10
        ) = 0 THEN 1
        ELSE 0
      END
    ) over(
      PARTITION BY date_day
      ORDER BY
        _id
    ) AS group_id
  FROM
    ids_days
)
SELECT
  date_day,
  fromTime,
  toTime,
  group_id,
  _inserted_timestamp,
  MIN(_id) AS min_id,
  MAX(_id) AS max_id,
  min_id || '-' || max_id AS id_range
FROM
  groupings
GROUP BY
  date_day,
  fromTime,
  toTime,
  group_id,
  _inserted_timestamp
