{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = 'block_timestamp::DATE',
) }}

WITH xfer_base AS (

    SELECT
        A.block_id AS block_number,
        A.block_timestamp,
        A.tx_id AS tx_hash,
        A.msg_index,
        SPLIT_PART(
            sender,
            '1',
            1
        ) AS source_chain,
        A.sender,
        A.foreign_chain destination_chain,
        A.foreign_address AS receiver,
        currency AS token_address,
        A.amount AS raw_amount,
        A.receiver AS deposit_address
    FROM
        {{ ref('silver__transfers') }} A
    WHERE
        A.foreign_address IS NOT NULL

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
    SELECT
        MAX(block_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
transfer_det AS (
    SELECT
        block_timestamp,
        tx_id,
        raw_amount,
        raw_denom,
        deposit_address,
        transfer_id,
        txid,
        raw_amount_received,
        raw_amount_denom,
        raw_fee_paid,
        raw_fee_denom
    FROM
        {{ ref('silver__transfer_details') }} A

{% if is_incremental() %}
WHERE
    block_timestamp :: DATE >= (
        SELECT
            MAX(block_timestamp) :: DATE - 2
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    A.block_number,
    A.block_timestamp,
    A.tx_hash,
    A.source_chain,
    A.sender,
    LOWER(
        A.destination_chain
    ) AS destination_chain,
    A.receiver AS receiver,
    A.token_address,
    A.raw_amount,
    A.deposit_address AS deposit_address,
    b.transfer_id,
    b.txid AS destination_tx_hash,
    raw_amount_received,
    raw_amount_denom,
    raw_fee_paid,
    raw_fee_denom,
    concat_ws(
        '-',
        A.tx_hash,
        A.msg_index
    ) AS _unique_key
FROM
    xfer_base A
    LEFT JOIN transfer_det b
    ON A.receiver = b.deposit_address
    AND A.raw_amount = b.raw_amount
    AND A.token_address = b.raw_denom
    AND A.block_timestamp <= b.block_timestamp qualify(ROW_NUMBER () over(PARTITION BY _unique_key
ORDER BY
    DATEDIFF(SECOND, A.block_timestamp, b.block_timestamp)) = 1)
