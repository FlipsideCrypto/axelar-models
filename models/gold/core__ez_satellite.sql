{{ config(
    materialized = 'view'
) }}

WITH axel_dec AS (

    SELECT
        COALESCE (
            raw_metadata [0] :aliases [0] :: STRING,
            raw_metadata [1] :denom
        ) AS token_address,
        CASE
            WHEN raw_metadata [0] :account_address IS NOT NULL THEN NULL
            ELSE COALESCE(
                raw_metadata [1] :exponent :: INT,
                6
            )
        END AS decimals,
        project_name AS token_symbol
    FROM
        {{ source(
            'osmo',
            'asset_metadata'
        ) }}
        b
    UNION
    SELECT
        'uusd',
        6,
        'UST'
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    source_chain,
    sender,
    destination_chain,
    receiver,
    A.token_address,
    b.token_symbol,
    CASE
        WHEN b.decimals IS NOT NULL THEN raw_amount / pow(
            10,
            b.decimals
        )
        ELSE NULL
    END AS amount,
    deposit_address,
    A.transfer_id,
    CASE
        WHEN b.decimals IS NOT NULL THEN raw_amount_received / pow(
            10,
            b.decimals
        )
        ELSE NULL
    END AS amount_received,
    raw_amount_denom AS amount_received_denom,
    CASE
        WHEN b.decimals IS NOT NULL THEN raw_fee_paid / pow(
            10,
            b.decimals
        )
        ELSE NULL
    END AS fee_paid,
    raw_fee_denom AS fee_denom
FROM
    {{ ref('silver__satellite_axelar') }} A
    LEFT JOIN axel_dec b
    ON A.token_address = b.token_address
    LEFT JOIN axel_dec C
    ON A.raw_amount_denom = b.token_address
    LEFT JOIN axel_dec d
    ON A.raw_fee_denom = b.token_address
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    source_chain,
    sender,
    destination_chain,
    receiver,
    token_address,
    b.symbol AS token_symbol,
    CASE
        WHEN decimals IS NOT NULL THEN raw_amount / pow(
            10,
            decimals
        )
        ELSE NULL
    END AS amount,
    deposit_address,
    NULL AS transfer_id,
    NULL AS amount_received,
    NULL AS amount_received_denom,
    NULL AS fee_paid,
    NULL AS fee_denom
FROM
    {{ ref('silver__satellite_arbitrum') }} A
    LEFT JOIN {{ source(
        'arbitrum',
        'dim_contracts'
    ) }}
    b
    ON A.token_address = b.address
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    source_chain,
    sender,
    destination_chain,
    receiver,
    token_address,
    b.symbol AS token_symbol,
    CASE
        WHEN decimals IS NOT NULL THEN raw_amount / pow(
            10,
            decimals
        )
        ELSE NULL
    END AS amount,
    deposit_address,
    NULL AS transfer_id,
    NULL AS amount_received,
    NULL AS amount_received_denom,
    NULL AS fee_paid,
    NULL AS fee_denom
FROM
    {{ ref('silver__satellite_avalanche') }} A
    LEFT JOIN {{ source(
        'avalanche',
        'dim_contracts'
    ) }}
    b
    ON A.token_address = b.address
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    source_chain,
    sender,
    destination_chain,
    receiver,
    A.token_address,
    CASE
        A.token_address
        WHEN '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c' THEN 'WBNB'
        WHEN '0x651fca96c77f5f988e2ca449b6e3a445399e2492' THEN 'axlAPE'
        WHEN '0x4268b8f0b87b6eae5d897996e6b845ddbd99adf3' THEN 'axlUSDC'
        WHEN '0x8b1f4432f943c465a973fedc6d7aa50fc96f1f65' THEN 'AXL'
    END AS token_symbol,
    CASE
        WHEN decimals IS NOT NULL THEN raw_amount / pow(
            10,
            decimals
        )
        ELSE NULL
    END AS amount,
    deposit_address,
    NULL AS transfer_id,
    NULL AS amount_received,
    NULL AS amount_received_denom,
    NULL AS fee_paid,
    NULL AS fee_denom
FROM
    {{ ref('silver__satellite_bsc') }} A
    LEFT JOIN (
        SELECT
            '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c' AS token_address,
            18 decimals
        UNION ALL
        SELECT
            '0x651fca96c77f5f988e2ca449b6e3a445399e2492' AS token_address,
            18 decimals
        UNION ALL
        SELECT
            '0x4268b8f0b87b6eae5d897996e6b845ddbd99adf3' AS token_address,
            6 decimals
        UNION ALL
        SELECT
            '0x8b1f4432f943c465a973fedc6d7aa50fc96f1f65' AS token_address,
            6 decimals
    ) b
    ON A.token_address = b.token_address
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    source_chain,
    sender,
    destination_chain,
    receiver,
    token_address,
    b.symbol AS token_symbol,
    CASE
        WHEN decimals IS NOT NULL THEN raw_amount / pow(
            10,
            decimals
        )
        ELSE NULL
    END AS amount,
    deposit_address,
    NULL AS transfer_id,
    NULL AS amount_received,
    NULL AS amount_received_denom,
    NULL AS fee_paid,
    NULL AS fee_denom
FROM
    {{ ref('silver__satellite_ethereum') }} A
    LEFT JOIN {{ source(
        'ethereum',
        'dim_contracts'
    ) }}
    b
    ON A.token_address = b.address
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    source_chain,
    sender,
    destination_chain,
    receiver,
    token_address,
    b.symbol AS token_symbol,
    CASE
        WHEN decimals IS NOT NULL THEN raw_amount / pow(
            10,
            decimals
        )
        ELSE NULL
    END AS amount,
    deposit_address,
    NULL AS transfer_id,
    NULL AS amount_received,
    NULL AS amount_received_denom,
    NULL AS fee_paid,
    NULL AS fee_denom
FROM
    {{ ref('silver__satellite_polygon') }} A
    LEFT JOIN {{ source(
        'polygon',
        'dim_contracts'
    ) }}
    b
    ON A.token_address = b.address
