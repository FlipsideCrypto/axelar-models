{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'delete+insert',
    cluster_by = 'block_timestamp::DATE',
) }}

WITH axelar_txs AS (

    SELECT
        DISTINCT tx_id
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        attribute_value IN (
            '/cosmos.bank.v1beta1.MsgSend',
            '/cosmos.bank.v1beta1.MsgMultiSend'
        )

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
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
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
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
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2 
    FROM
        {{ this }}
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
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
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
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2 
    FROM
        {{ this }}
)
{% endif %}
),
axelar_txs_final AS (
    SELECT
        block_id,
        block_timestamp,
        r.tx_id,
        tx_succeeded,
        'AXELAR' AS transfer_type,
        r.msg_index,
        sender,
        amount,
        currency,
        COALESCE(
            l.raw_metadata [1] :exponent,
            6
        ) AS DECIMAL,
        receiver,
        _inserted_timestamp,
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
        LEFT OUTER JOIN {{ ref('core__dim_labels') }}
        l
        ON currency = l.address

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
ibc_in_tx AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        msg_index,
        'IBC_TRANSFER_IN' AS transfer_type,
        TRY_PARSE_JSON(attribute_value) :sender :: STRING AS sender,
        TRY_PARSE_JSON(attribute_value) :amount :: INT AS amount,
        CASE
            WHEN TRY_PARSE_JSON(attribute_value) :denom :: STRING LIKE '%/%' THEN SPLIT(TRY_PARSE_JSON(attribute_value) :denom :: STRING, '/') [array_size(split(try_parse_json(attribute_value):denom::string, '/')) - 1]
            ELSE TRY_PARSE_JSON(attribute_value) :denom :: STRING
        END AS currency,
        TRY_PARSE_JSON(attribute_value) :receiver :: STRING AS receiver,
        _inserted_timestamp,
        concat_ws(
            '-',
            tx_id,
            msg_index,
            currency
        ) AS _unique_key
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type = 'write_acknowledgement'
        AND attribute_key = 'packet_data'
        AND TRY_PARSE_JSON(attribute_value): amount IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
ibc_out_txid AS (
    SELECT
        tx_id
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type = 'ibc_transfer'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2 
    FROM
        {{ this }}
)
{% endif %}
),
ibc_out_tx AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        msg_index,
        'IBC_TRANSFER_OUT' AS transfer_type,
        TRY_PARSE_JSON(attribute_value) :sender :: STRING AS sender,
        TRY_PARSE_JSON(attribute_value) :amount :: INT AS amount,
        CASE
            WHEN TRY_PARSE_JSON(attribute_value) :denom :: STRING LIKE '%/%' THEN SPLIT(TRY_PARSE_JSON(attribute_value) :denom :: STRING, '/') [array_size(split(try_parse_json(attribute_value):denom::string, '/')) - 1]
            ELSE TRY_PARSE_JSON(attribute_value) :denom :: STRING
        END AS currency,
        TRY_PARSE_JSON(attribute_value) :receiver :: STRING AS receiver,
        _inserted_timestamp,
        concat_ws(
            '-',
            tx_id,
            msg_index,
            currency
        ) AS _unique_key
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                ibc_out_txid
        )
        AND msg_type = 'send_packet'
        AND attribute_key = 'packet_data'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2  
    FROM
        {{ this }}
)
{% endif %}
),
decimals AS (
    SELECT
        *,
        COALESCE (
            raw_metadata [0] :aliases [0] :: STRING,
            raw_metadata [1] :denom
        ) AS denom_name,
        CASE
            WHEN raw_metadata [0] :account_address IS NOT NULL THEN NULL
            ELSE COALESCE(
                raw_metadata [1] :exponent :: INT,
                6
            )
        END AS DECIMAL
    FROM
        {{ ref('core__dim_labels') }}
),
ibc_transfers_agg AS (
    SELECT
        *
    FROM
        ibc_out_tx
    UNION ALL
    SELECT
        *
    FROM
        ibc_in_tx
),
ibc_tx_final AS (
    SELECT
        i.block_id,
        i.block_timestamp,
        i.tx_id,
        i.tx_succeeded,
        i.transfer_type,
        i.sender,
        i.amount,
        i.currency,
        CASE
            WHEN i.currency IN (
                'uusd',
                'uosmo'
            ) THEN 6
            WHEN i.currency LIKE '%-wei' THEN 18
            ELSE d.decimal
        END AS DECIMAL,
        i.receiver,
        msg_index,
        _inserted_timestamp,
        _unique_key
    FROM
        ibc_transfers_agg i
        LEFT JOIN decimals d
        ON i.currency = d.denom_name
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    transfer_type,
    sender,
    amount,
    currency,
    DECIMAL,
    receiver,
    msg_index,
    _inserted_timestamp,
    _unique_key
FROM
    ibc_tx_final
UNION ALL
SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    transfer_type,
    sender,
    amount,
    currency,
    DECIMAL,
    receiver,
    msg_index,
    _inserted_timestamp,
    _unique_key
FROM
    axelar_txs_final
