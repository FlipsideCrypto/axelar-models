{{ config(
    materialized = 'incremental',
    unique_key = ["tx_id","msg_group","msg_sub_group"],
    incremental_strategy = 'merge',
    cluster_by = 'block_timestamp::DATE'
) }}

WITH base_atts AS (

    SELECT
        block_id,
        block_timestamp,
        tx_succeeded,
        tx_id,
        msg_group,
        msg_sub_group,
        msg_index,
        msg_type,
        attribute_key,
        attribute_value,
        b.key,
        b.value,
        _inserted_timestamp
    FROM
        {{ ref('silver__msg_attributes') }},
        LATERAL FLATTEN(TRY_PARSE_JSON(attribute_value), outer => TRUE) b
    WHERE
        (
            msg_type IN (
                'depositConfirmation',
                'axelar.axelarnet.v1beta1.TokenSent'
            )
            OR (
                msg_type = 'axelar.nexus.v1beta1.FeeDeducted'
                AND attribute_key IN (
                    'fee',
                    'amount'
                )
            )
            OR (
                msg_type = 'write_acknowledgement'
                AND b.key = 'receiver'
            )
        )
        AND attribute_key NOT IN (
            'module',
            'action',
            'value'
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
excludes_InsufficientFee AS (
    SELECT
        DISTINCT tx_id,
        msg_group
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type = 'axelar.nexus.v1beta1.InsufficientFee'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
fin AS (
    SELECT
        block_id,
        block_timestamp,
        tx_succeeded,
        A.tx_id,
        A.msg_group,
        msg_sub_group,
        _inserted_timestamp,
        OBJECT_AGG(
            CASE
                WHEN key IS NOT NULL THEN attribute_key || '-' || key
                ELSE attribute_key
            END :: STRING,
            COALESCE(
                VALUE,
                attribute_value
            ) :: variant
        ) AS j,
        COALESCE(
            j :amount,
            j :"asset-amount"
        ) :: STRING AS raw_total_sent,
        j :"amount-amount" :: INT AS raw_amount_received,
        COALESCE(
            j :"amount-denom",
            j :"asset-denom"
        ) :: STRING AS raw_amount_denom,
        j :"fee-amount" :: INT AS raw_fee_paid,
        j :"fee-denom" :: STRING AS raw_fee_denom,
        COALESCE(
            j :depositAddress,
            j :"packet_data-receiver"
        ) :: STRING AS depositAddress,
        COALESCE(
            j :destinationAddress,
            j :destination_address
        ) :: STRING AS destinationAddress,
        COALESCE(
            j :destinationChain,
            j :destination_chain
        ) :: STRING AS destinationChain,
        COALESCE(
            j :sourceChain,
            j :source_chain
        ) :: STRING AS sourceChain,
        COALESCE(
            j :transferID,
            j :transfer_id
        ) :: STRING AS transferID,
        j :chain :: STRING AS chain,
        j :tokenAddress :: STRING AS tokenAddress,
        j :txID :: STRING AS txID,
        j :confHeight :: STRING AS confHeight
    FROM
        base_atts A
        LEFT JOIN excludes_InsufficientFee e
        ON A.tx_id = e.tx_id
        AND A.msg_group = e.msg_group
    WHERE
        e.msg_group IS NULL
    GROUP BY
        block_id,
        block_timestamp,
        tx_succeeded,
        A.tx_id,
        A.msg_group,
        msg_sub_group,
        _inserted_timestamp
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    msg_group,
    msg_sub_group,
    raw_total_sent,
    COALESCE(
        SPLIT_PART(
            TRIM(
                REGEXP_REPLACE(
                    raw_total_sent,
                    '[^[:digit:]]',
                    ' '
                )
            ),
            ' ',
            0
        ),
        raw_total_sent
    ) :: INT AS raw_amount,
    COALESCE(
        NULLIF(
            RIGHT(raw_total_sent, LENGTH(raw_total_sent) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(raw_total_sent, '[^[:digit:]]', ' ')), ' ', 0))),
            ''
        ),
        raw_amount_denom
    ) AS raw_denom,
    raw_amount_received,
    raw_amount_denom,
    raw_fee_paid,
    raw_fee_denom,
    REPLACE(
        depositAddress,
        '"'
    ) AS deposit_address,
    REPLACE(
        destinationAddress,
        '"'
    ) AS destination_address,
    REPLACE(
        destinationChain,
        '"'
    ) AS destination_chain,
    REPLACE(
        sourceChain,
        '"'
    ) AS source_chain,
    REPLACE(
        transferID,
        '"'
    ) AS transfer_id,
    chain,
    tokenAddress AS token_address,
    txID AS txid,
    confHeight AS conf_height,
    _inserted_timestamp
FROM
    fin
