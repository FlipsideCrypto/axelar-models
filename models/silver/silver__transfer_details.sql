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
            msg_type = 'depositConfirmation'
            OR (
                msg_type = 'axelar.nexus.v1beta1.FeeDeducted'
                AND attribute_key IN (
                    'fee',
                    'amount'
                )
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
fin AS (
    SELECT
        block_id,
        block_timestamp,
        tx_succeeded,
        tx_id,
        msg_group,
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
        j :amount :: STRING AS raw_total_sent,
        j :"amount-amount" :: INT AS raw_amount_received,
        j :"amount-denom" :: STRING AS raw_amount_denom,
        j :"fee-amount" :: INT AS raw_fee_paid,
        j :"fee-denom" :: STRING AS raw_fee_denom,
        j :depositAddress :: STRING AS depositAddress,
        j :destinationAddress :: STRING AS destinationAddress,
        j :destinationChain :: STRING AS destinationChain,
        j :sourceChain :: STRING AS sourceChain,
        j :transferID :: STRING AS transferID,
        j :chain :: STRING AS chain,
        j :tokenAddress :: STRING AS tokenAddress,
        j :txID :: STRING AS txID,
        j :confHeight :: STRING AS confHeight
    FROM
        base_atts
    GROUP BY
        block_id,
        block_timestamp,
        tx_succeeded,
        tx_id,
        msg_group,
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
    RIGHT(raw_total_sent, LENGTH(raw_total_sent) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(raw_total_sent, '[^[:digit:]]', ' ')), ' ', 0))) AS raw_denom,
    raw_amount_received,
    raw_amount_denom,
    raw_fee_paid,
    raw_fee_denom,
    depositAddress AS deposit_address,
    destinationAddress AS destination_address,
    destinationChain AS destination_chain,
    sourceChain AS source_chain,
    transferID AS transfer_id,
    chain,
    tokenAddress AS token_address,
    txID AS txid,
    confHeight AS conf_height,
    _inserted_timestamp
FROM
    fin
