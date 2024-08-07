{{ config(
  materialized = 'incremental',
  unique_key = "tx_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_id,tx_from);",
  tags = ['core']
) }}

WITH

{% if is_incremental() %}
max_block_partition AS (

  SELECT
    MAX(
      _inserted_timestamp
    ) :: DATE - 2 AS max_date
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
    AND fee LIKE '%uaxl'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    max_date
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
AND _inserted_timestamp :: DATE >= (
  SELECT
    max_date
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
    t.tx_id,
    s.tx_from,
    tx_succeeded,
    codespace,
    COALESCE(
      fee,
      '0uaxl'
    ) AS fee_raw,
    REGEXP_SUBSTR(
      fee_raw,
      '[0-9]+'
    ) AS fee,
    REGEXP_SUBSTR(
      fee_raw,
      '[a-z]+'
    ) AS fee_denom,
    gas_used,
    gas_wanted,
    tx_code,
    tx_log,
    msgs,
    _inserted_timestamp
  FROM
    {{ ref('silver__transactions') }}
    t
    LEFT OUTER JOIN fee f
    ON t.tx_id = f.tx_id
    INNER JOIN spender s
    ON t.tx_id = s.tx_id

{% if is_incremental() %}
WHERE
  _inserted_timestamp :: DATE >= (
    SELECT
      max_date
    FROM
      max_block_partition
  )
{% endif %}
)
SELECT
  block_id,
  block_timestamp,
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
  _inserted_timestamp,
  {{ dbt_utils.generate_surrogate_key(
    ['tx_id']
  ) }} AS transactions_final_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  final_transactions
