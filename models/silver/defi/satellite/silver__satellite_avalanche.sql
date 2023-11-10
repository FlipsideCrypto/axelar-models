{{ config(
    materialized = 'incremental',
    unique_key = "tx_hash",
    incremental_strategy = 'merge',
    cluster_by = 'block_timestamp::DATE',
    tags = ['noncore']
) }}

WITH xfer_base AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        from_address,
        to_address,
        contract_address,
        raw_amount,
        origin_from_address
    FROM
        {{ source(
            'avalanche_silver',
            'transfers'
        ) }}
    WHERE
        block_timestamp :: DATE >= '2021-12-30'

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
    SELECT
        MAX(block_timestamp) :: DATE -2
    FROM
        {{ this }}
)
{% endif %}
),
links AS (
    SELECT
        tx_id,
        source_Chain AS sourceChain,
        LOWER(deposit_Address) AS depositAddress,
        LOWER(
            j :tokenAddress :: STRING
        ) AS tokenAddress,
        destination_Chain AS destinationChain,
        LOWER(destination_Address) AS destinationAddress
    FROM
        {{ ref('silver__link_events') }}

{% if is_incremental() %}
WHERE
    block_timestamp :: DATE >= (
        SELECT
            MAX(block_timestamp) :: DATE - 2
        FROM
            {{ this }}
    )
{% endif %}
),
transfer_amount_on_arb AS (
    SELECT
        tx_hash,
        from_address AS depositAddress,
        contract_address AS tokenaddress,
        raw_amount
    FROM
        xfer_base A
        JOIN links b
        ON A.contract_address = b.tokenAddress
        AND A.from_address = b.depositAddress
    WHERE
        to_address IN (
            LOWER('0x4F4495243837681061C4743b74B3eEdf548D56A5'),
            '0x0000000000000000000000000000000000000000'
        )
),
id_transferer AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash AS og_TX_HASH,
        origin_from_address AS eoa,
        to_address AS depositAddress,
        contract_address AS tokenaddress,
        raw_amount
    FROM
        xfer_base A
        JOIN links b
        ON A.contract_address = b.tokenAddress
        AND A.to_address = b.depositAddress
),
labeled_transfer_amount AS (
    SELECT
        *
    FROM
        id_transferer
        LEFT JOIN transfer_amount_on_arb USING (
            depositAddress,
            tokenaddress,
            raw_amount
        )
)
SELECT
    block_number,
    block_timestamp,
    og_tx_hash AS tx_hash,
    LOWER(
        COALESCE(
            sourcechain,
            'avalanche'
        )
    ) AS source_chain,
    eoa AS sender,
    LOWER(destinationchain) AS destination_chain,
    destinationaddress AS receiver,
    {# og_tx_hash,
    tx_hash,
    #}
    tokenaddress AS token_address,
    raw_amount,
    depositaddress AS deposit_address
FROM
    links
    JOIN labeled_transfer_amount USING (depositAddress)
WHERE
    COALESCE(
        raw_amount,
        0
    ) <> 0
