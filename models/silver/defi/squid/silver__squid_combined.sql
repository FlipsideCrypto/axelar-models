{{ config(
    materialized = 'table',
    cluster_by = ['block_timestamp::DATE'],
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'SQUID',
    'PURPOSE': 'DEFI' }} },
    tags = ['noncore']
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
            WHEN decimals IS NOT NULL THEN raw_amount / pow(
                10,
                decimals
            )
            ELSE NULL
        END AS amount,
        'arbitrum' AS source_chain,
        destination_chain,
        receiver,
        squid_arbitrum_id AS squid_combined_id,
        inserted_timestamp,
        modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        {{ ref('silver__squid_arbitrum') }} A
        LEFT JOIN {{ ref(
            'silver__evm_contracts'
        ) }}
        b
        ON b.blockchain = 'arbitrum'
        AND A.token_address = b.address
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
        'avalanche' AS source_chain,
        destination_chain,
        receiver,
        squid_avalanche_id AS squid_combined_id,
        inserted_timestamp,
        modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        {{ ref('silver__squid_avalanche') }} A
        LEFT JOIN {{ ref(
            'silver__evm_contracts'
        ) }}
        b
        ON b.blockchain = 'avalanche'
        AND A.token_address = b.address
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
        'binance' AS source_chain,
        destination_chain,
        receiver,
        squid_bsc_id AS squid_combined_id,
        inserted_timestamp,
        modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        {{ ref('silver__squid_bsc') }} A
        LEFT JOIN {{ ref(
            'silver__evm_contracts'
        ) }}
        b
        ON b.blockchain = 'bsc'
        AND A.token_address = b.address
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
        receiver,
        squid_ethereum_id AS squid_combined_id,
        inserted_timestamp,
        modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        {{ ref('silver__squid_ethereum') }} A
        LEFT JOIN {{ ref(
            'silver__evm_contracts'
        ) }}
        b
        ON b.blockchain = 'ethereum'
        AND A.token_address = b.address
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
        'polygon' AS source_chain,
        destination_chain,
        receiver,
        squid_polygon_id AS squid_combined_id,
        inserted_timestamp,
        modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        {{ ref('silver__squid_polygon') }} A
        LEFT JOIN {{ ref(
            'silver__evm_contracts'
        ) }}
        b
        ON b.blockchain = 'polygon'
        AND A.token_address = b.address
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
        'optimism' AS source_chain,
        destination_chain,
        receiver,
        squid_optimism_id AS squid_combined_id,
        inserted_timestamp,
        modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        {{ ref('silver__squid_optimism') }} A
        LEFT JOIN {{ ref(
            'silver__evm_contracts'
        ) }}
        b
        ON b.blockchain = 'optimism'
        AND A.token_address = b.address
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
        'base' AS source_chain,
        destination_chain,
        receiver,
        squid_base_id AS squid_combined_id,
        inserted_timestamp,
        modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        {{ ref('silver__squid_base') }} A
        LEFT JOIN {{ ref(
            'silver__evm_contracts'
        ) }}
        b
        ON b.blockchain = 'base'
        AND A.token_address = b.address
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
    LTRIM(
        REGEXP_REPLACE(
            receiver,
            '[\x00-\x1F\x7F-\x9F\xAD]',
            '',
            1
        )
    ) AS receiver,
    squid_combined_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM
    base
