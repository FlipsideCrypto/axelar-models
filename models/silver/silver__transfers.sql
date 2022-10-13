{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = 'block_timestamp::DATE',
) }}

WITH axelar_txs AS (

    SELECT
        DISTINCT tx_id
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        attribute_value in (
             '/cosmos.bank.v1beta1.MsgSend'
             ,'/cosmos.bank.v1beta1.MsgMultiSend'
        )

{% if is_incremental() %}
AND _partition_by_block_id >= (
    SELECT
        MAX(_partition_by_block_id) -1
    FROM
        {{ this }}
)
AND _partition_by_block_id <= (
    SELECT
        MAX(_partition_by_block_id) + 10
    FROM
        {{ this }}
)
{% else %}
    AND _partition_by_block_id IN (
        0,
        1
    )
{% endif %}
),
sender AS (
    SELECT
        tx_id,
        msg_index,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS sender
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type = 'tx'
        AND attribute_key = 'acc_seq'

{% if is_incremental() %}
AND _partition_by_block_id >= (
    SELECT
        MAX(_partition_by_block_id) -1
    FROM
        {{ this }}
)
AND _partition_by_block_id <= (
    SELECT
        MAX(_partition_by_block_id) + 10
    FROM
        {{ this }}
)
{% else %}
    AND _partition_by_block_id IN (
        0,
        1
    )
{% endif %}
),
msg_index AS (
    SELECT
        v.tx_id,
        attribute_key,
        m.msg_index
    FROM
        axelar_txs v
        LEFT OUTER JOIN {{ ref('silver__msg_attributes') }}
        m
        ON v.tx_id = m.tx_id
        INNER JOIN sender s
        ON v.tx_id = s.tx_id
    WHERE
        msg_type = 'transfer'
        AND attribute_key = 'amount'
        AND m.msg_index > s.msg_index

{% if is_incremental() %}
AND _partition_by_block_id >= (
    SELECT
        MAX(_partition_by_block_id) -1
    FROM
        {{ this }}
)
AND _partition_by_block_id <= (
    SELECT
        MAX(_partition_by_block_id) + 10
    FROM
        {{ this }}
)
{% else %}
    AND _partition_by_block_id IN (
        0,
        1
    )
{% endif %}
),
receiver AS (
    SELECT
        v.tx_id,
        m.msg_index,
        attribute_value AS receiver
    FROM
        axelar_txs v
        LEFT OUTER JOIN {{ ref('silver__msg_attributes') }}
        m
        ON v.tx_id = m.tx_id
        INNER JOIN sender s
        ON v.tx_id = s.tx_id
    WHERE
        msg_type = 'transfer'
        AND attribute_key = 'recipient'
        AND m.msg_index > s.msg_index

{% if is_incremental() %}
AND _partition_by_block_id >= (
    SELECT
        MAX(_partition_by_block_id) -1
    FROM
        {{ this }}
)
AND _partition_by_block_id <= (
    SELECT
        MAX(_partition_by_block_id) + 10
    FROM
        {{ this }}
)
{% else %}
    AND _partition_by_block_id IN (
        0,
        1
    )
{% endif %}
),
amount AS (
    SELECT
        v.tx_id,
        m.msg_index,
        COALESCE(
            SPLIT_PART(
                TRIM(
                    REGEXP_REPLACE(
                        attribute_value,
                        '[^[:digit:]]',
                        ' '
                    )
                ),
                ' ',
                0
            ),
            TRY_PARSE_JSON(attribute_value) :amount
        ) AS amount,
        COALESCE(
            RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))),
            TRY_PARSE_JSON(attribute_value) [1] :denom
        ) AS currency
    FROM
        axelar_txs v
        LEFT OUTER JOIN {{ ref('silver__msg_attributes') }}
        m
        ON v.tx_id = m.tx_id
        INNER JOIN sender s
        ON v.tx_id = s.tx_id
    WHERE
        msg_type = 'transfer'
        AND attribute_key = 'amount'
        AND m.msg_index > s.msg_index

{% if is_incremental() %}
AND _partition_by_block_id >= (
    SELECT
        MAX(_partition_by_block_id) -1
    FROM
        {{ this }}
)
AND _partition_by_block_id <= (
    SELECT
        MAX(_partition_by_block_id) + 10
    FROM
        {{ this }}
)
{% else %}
    AND _partition_by_block_id IN (
        0,
        1
    )
{% endif %}
)
SELECT
    block_id,
    block_timestamp,
    blockchain,
    chain_id,
    r.tx_id,
    tx_succeeded,
    'AXELAR' AS transfer_type,
    r.msg_index,
    sender,
    amount,
    currency,
    receiver,
    _partition_by_block_id,
    concat_ws(
        '-',
        r.tx_id,
        r.msg_index,
        currency
    ) AS _unique_key
FROM
    receiver r
    LEFT OUTER JOIN amount C
    ON r.tx_id = C.tx_id
    AND r.msg_index = C.msg_index
    LEFT OUTER JOIN sender s
    ON r.tx_id = s.tx_id
    LEFT OUTER JOIN {{ ref('silver__transactions') }}
    t
    ON r.tx_id = t.tx_id

{% if is_incremental() %}
WHERE
    _partition_by_block_id >= (
        SELECT
            MAX(_partition_by_block_id) -1
        FROM
            {{ this }}
    )
    AND _partition_by_block_id <= (
        SELECT
            MAX(_partition_by_block_id) + 10
        FROM
            {{ this }}
    )
{% else %}
WHERE
    _partition_by_block_id IN (
        0,
        1
    )
{% endif %}
