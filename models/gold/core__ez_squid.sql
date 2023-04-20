{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'SQUID',
    'PURPOSE': 'DEFI' }} }
) }}

WITH base AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        sender,
        A.token_address,
        A.token_symbol,
        CASE
            WHEN token_decimals IS NOT NULL THEN raw_amount / pow(
                10,
                token_decimals
            )
            ELSE NULL
        END AS amount,
        'arbitrum' AS source_chain,
        destination_chain,
        receiver
    FROM
        {{ ref('silver__squid_arbitrum') }} A
        LEFT JOIN {{ source(
            'arbitrum_silver',
            'contracts'
        ) }}
        b
        ON A.token_address = b.contract_address
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        sender,
        A.token_address,
        A.token_symbol,
        CASE
            WHEN token_decimals IS NOT NULL THEN raw_amount / pow(
                10,
                token_decimals
            )
            ELSE NULL
        END AS amount,
        'avalanche' AS source_chain,
        destination_chain,
        receiver
    FROM
        {{ ref('silver__squid_avalanche') }} A
        LEFT JOIN {{ source(
            'avalanche_silver',
            'contracts'
        ) }}
        b
        ON A.token_address = b.contract_address
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        sender,
        A.token_address,
        A.token_symbol,
        CASE
            WHEN token_symbol = 'axlUSDC' THEN raw_amount / pow(
                10,
                6
            )
            ELSE NULL
        END AS amount,
        'binance' AS source_chain,
        destination_chain,
        receiver
    FROM
        {{ ref('silver__squid_bsc') }} A
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        sender,
        A.token_address,
        A.token_symbol,
        CASE
            WHEN decimals IS NOT NULL THEN raw_amount / pow(
                10,
                decimals
            )
            ELSE NULL
        END AS amount,
        'ethereum' AS source_chain,
        destination_chain,
        receiver
    FROM
        {{ ref('silver__squid_ethereum') }} A
        LEFT JOIN {{ source(
            'ethereum_silver',
            'contracts'
        ) }}
        b
        ON A.token_address = b.address
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        sender,
        A.token_address,
        A.token_symbol,
        CASE
            WHEN token_decimals IS NOT NULL THEN raw_amount / pow(
                10,
                token_decimals
            )
            ELSE NULL
        END AS amount,
        'polygon' AS source_chain,
        destination_chain,
        receiver
    FROM
        {{ ref('silver__squid_polygon') }} A
        LEFT JOIN {{ source(
            'polygon_silver',
            'contracts'
        ) }}
        b
        ON A.token_address = b.contract_address
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    sender,
    token_address,
    token_symbol,
    amount,
    source_chain,
    CASE
        WHEN destination_chain = 'avalanch' THEN 'avalanche'
        ELSE destination_chain
    END AS destination_chain,
    receiver
FROM
    base
