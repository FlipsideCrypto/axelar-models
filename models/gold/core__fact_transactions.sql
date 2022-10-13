{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE'],
) }}

with  {% if is_incremental() %}
max_block_partition AS (

    SELECT
        MAX(
            _partition_by_block_id
        ) as _partition_by_block_id_max
        
        from {{ ref('silver__transactions') }}
    
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

{% if is_incremental() %}
AND
  _partition_by_block_id >= (
    SELECT
      _partition_by_block_id_max -1
    FROM
      max_block_partition
  )
  AND _partition_by_block_id <= (
    SELECT
      _partition_by_block_id_max + 10
    FROM
      max_block_partition
  )
{% else %}
AND
  _partition_by_block_id IN (
    0,
    1
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
AND
  _partition_by_block_id >= (
    SELECT
      _partition_by_block_id_max -1
    FROM
      max_block_partition
  )
  AND _partition_by_block_id <= (
    SELECT
      _partition_by_block_id_max + 10
    FROM
      max_block_partition
  )
{% else %}
AND
  _partition_by_block_id IN (
    0,
    1
  )
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY tx_id
ORDER BY
    msg_index)) = 1
)


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
    ) AS fee,
    gas_used,
    gas_wanted,
    tx_code,
    tx_log,
    msgs,
    _partition_by_block_id

    from {{ ref('silver__transactions') }}
    t
    LEFT OUTER JOIN fee f
    ON t.tx_id = f.tx_id
    LEFT OUTER JOIN spender s
    ON t.tx_id = s.tx_id

{% if is_incremental() %}
WHERE
  _partition_by_block_id >= (
    SELECT
      _partition_by_block_id_max -1
    FROM
      max_block_partition
  )
  AND _partition_by_block_id <= (
    SELECT
      _partition_by_block_id_max + 10
    FROM
      max_block_partition
  )
{% else %}
WHERE
  _partition_by_block_id IN (
    0,
    1
  )
{% endif %}





