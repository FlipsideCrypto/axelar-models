{{ config(
  materialized = 'incremental',
  unique_key = "tx_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
) }}

WITH

{% if is_incremental() %}
max_block_partition AS (

  SELECT
    MAX(
      _partition_by_block_id
    ) AS _partition_by_block_id_max
  FROM
    {{ ref('silver__transactions') }}
),
{% endif %}

fee AS (
  SELECT
    tx_id,
    attribute_value AS fee
  FROM
    {{ ref('silver__msg_attributes') }}
  WHERE
    attribute_key = 'fee'
    AND fee like '%uaxl'

{% if is_incremental() %}
AND _partition_by_block_id >= (
  SELECT
    _partition_by_block_id_max 
  FROM
    max_block_partition
)
{% endif %}
),

spender AS (
  SELECT
    tx_id,
    SPLIT_PART(
      attribute_value,
      '/',
      0
    ) AS tx_from
  FROM
    {{ ref('silver__msg_attributes') }}
  WHERE
    attribute_key = 'acc_seq'

{% if is_incremental() %}
AND _partition_by_block_id >= (
  SELECT
    _partition_by_block_id_max
  FROM
    max_block_partition
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY tx_id
ORDER BY
  msg_index)) = 1
),

final_transactions AS (
SELECT
  t.block_id,
  t.block_timestamp,
  t.blockchain,
  t.chain_id,
  t.tx_id,
  s.tx_from,
  tx_succeeded,
  codespace,
  COALESCE(
    fee,
    '0uaxl'
  ) AS fee_raw,
  regexp_substr(fee_raw, '[0-9]+') as fee, 
  regexp_substr(fee_raw, '[a-z]+') as fee_denom, 
  gas_used,
  gas_wanted,
  tx_code,
  tx_log,
  msgs,
  _partition_by_block_id
FROM
  {{ ref('silver__transactions') }}
  t
  LEFT OUTER JOIN fee f
  ON t.tx_id = f.tx_id
  INNER JOIN spender s
  ON t.tx_id = s.tx_id

{% if is_incremental() %}
WHERE
  _partition_by_block_id >= (
    SELECT
      _partition_by_block_id_max
    FROM
      max_block_partition
  )
{% endif %}
)

SELECT 
  block_id,
  block_timestamp,
  blockchain,
  chain_id,
  tx_id,
  tx_from,
  tx_succeeded,
  codespace,
  fee, 
  fee_denom, 
  gas_used,
  gas_wanted,
  tx_code,
  tx_log,
  msgs,
  _partition_by_block_id

FROM final_transactions


