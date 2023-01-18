{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'delete+insert',
    cluster_by = 'block_timestamp::DATE',
) }}

WITH base_atts AS (

    SELECT
        block_id,
        block_timestamp,
        blockchain,
        chain_id,
        tx_id,
        tx_succeeded,
        _inserted_timestamp,
        msg_group,
        msg_index,
        msg_type,
        attribute_key,
        attribute_value
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        (
            msg_type = 'message'
            AND attribute_key = 'action'
            AND attribute_value IN (
                '/cosmos.bank.v1beta1.MsgSend',
                '/cosmos.bank.v1beta1.MsgMultiSend'
            )
            OR (
                msg_type = 'tx'
                AND attribute_key = 'acc_seq'
            )
            OR (
                msg_type = 'transfer'
                AND attribute_key IN (
                    'amount',
                    'recipient',
                    'sender'
                )
            )
            OR (
                msg_type = 'write_acknowledgement'
                AND attribute_key = 'packet_data'
                AND TRY_PARSE_JSON(attribute_value): amount IS NOT NULL
            )
            OR (
                msg_type = 'ibc_transfer'
            )
            OR (
                msg_type = 'send_packet'
                AND attribute_key = 'packet_data'
            )
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
        base_atts
    WHERE
        msg_type = 'tx'
        AND attribute_key = 'acc_seq'
),
axelar_txs AS (
    SELECT
        DISTINCT tx_id
    FROM
        base_atts
    WHERE
        msg_type = 'message'
        AND attribute_key = 'action'
        AND attribute_value IN (
            '/cosmos.bank.v1beta1.MsgSend',
            '/cosmos.bank.v1beta1.MsgMultiSend'
        )
),
transfer_values AS (
    SELECT
        v.tx_id,
        m.msg_index,
        OBJECT_AGG(
            m.attribute_key :: STRING,
            m.attribute_value :: variant
        ) AS j,
        j :sender :: STRING AS sender,
        j :recipient :: STRING AS receiver,
        COALESCE(
            SPLIT_PART(
                TRIM(
                    REGEXP_REPLACE(
                        j :amount,
                        '[^[:digit:]]',
                        ' '
                    )
                ),
                ' ',
                0
            ),
            TRY_PARSE_JSON(
                j :amount
            ) :amount
        ) AS amount,
        COALESCE(
            RIGHT(j :amount, LENGTH(j :amount) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(j :amount, '[^[:digit:]]', ' ')), ' ', 0))),
            TRY_PARSE_JSON(
                j :amount
            ) [1] :denom
        ) AS currency
    FROM
        axelar_txs v
        JOIN base_atts m
        ON v.tx_id = m.tx_id
    WHERE
        m.msg_type = 'transfer'
        AND m.attribute_key IN(
            'amount',
            'recipient',
            'sender'
        )
        AND msg_group IS NOT NULL
    GROUP BY
        v.tx_id,
        m.msg_index
),
axelar_txs_final AS (
    SELECT
        block_id,
        block_timestamp,
        t.blockchain,
        chain_id,
        r.tx_id,
        tx_succeeded,
        CASE
            WHEN len(receiver) = 65 THEN 'IBC_TRANSFER_OUT'
            WHEN len(
                COALESCE(
                    r.sender,
                    snd.sender
                )
            ) = 65 THEN 'IBC_TRANSFER_IN'
            ELSE 'AXELAR'
        END AS transfer_type,
        r.msg_index,
        COALESCE(
            r.sender,
            snd.sender
        ) AS sender,
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
        transfer_values r
        JOIN (
            SELECT
                DISTINCT block_id,
                block_timestamp,
                blockchain,
                chain_id,
                tx_id,
                tx_succeeded,
                _inserted_timestamp
            FROM
                base_atts
        ) t
        ON r.tx_id = t.tx_id
        LEFT OUTER JOIN {{ ref('core__dim_labels') }}
        l
        ON currency = l.address
        LEFT JOIN sender snd
        ON r.tx_id = snd.tx_id
),
ibc_in_tx AS (
    SELECT
        block_id,
        block_timestamp,
        blockchain,
        chain_id,
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
        base_atts
    WHERE
        msg_type = 'write_acknowledgement'
        AND attribute_key = 'packet_data'
        AND TRY_PARSE_JSON(attribute_value): amount IS NOT NULL
),
ibc_out_txid AS (
    SELECT
        tx_id
    FROM
        base_atts
    WHERE
        msg_type = 'ibc_transfer'
),
ibc_out_tx AS (
    SELECT
        block_id,
        block_timestamp,
        blockchain,
        chain_id,
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
        base_atts
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                ibc_out_txid
        )
        AND msg_type = 'send_packet'
        AND attribute_key = 'packet_data'
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
        i.blockchain,
        i.chain_id,
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
),
fin AS (
    SELECT
        block_id,
        block_timestamp,
        blockchain,
        chain_id,
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
        blockchain,
        chain_id,
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
)
SELECT
    A.block_id,
    A.block_timestamp,
    A.blockchain,
    A.chain_id,
    A.tx_id,
    A.tx_succeeded,
    A.transfer_type,
    A.sender,
    A.amount,
    A.currency,
    A.decimal,
    A.receiver,
    A.msg_index,
    b.destination_address AS foreign_address,
    A._inserted_timestamp,
    A._unique_key
FROM
    fin A
    LEFT JOIN {{ ref('silver__link_events') }}
    b
    ON A.receiver = b.deposit_address
    AND len(receiver) = 65
