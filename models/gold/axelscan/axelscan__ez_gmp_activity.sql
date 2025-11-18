{{ config(
    materialized = 'incremental',
    unique_key = ['ez_gmp_activity_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['created_at::DATE'],
    tags = ['daily']
) }}

-- Enhanced Combined EZ GMP model that merges THREE complementary data sources:
-- 1. interchain_transfers ARRAY data - multiple transfers in one GMP (with embedded prices)
-- 2. interchain_transfer OBJECT data - single transfer in one GMP (with price lookups added)
-- 3. Standard GMP activity data - GMPs without interchain transfer details (with price lookups)
-- This is the DEFINITIVE model for all GMP transfer analysis

WITH
-- Token metadata with coingecko_id mapping
token_meta AS (
    SELECT
        token_symbol,
        coingecko_id
    FROM
        {{ ref('silver__axelscan_token_meta') }}
    WHERE
        coingecko_id IS NOT NULL
),

coingecko_prices AS (
    SELECT
        HOUR,
        asset_id AS coingecko_id,
        CLOSE AS price
    FROM
         {{ source('crosschain_price','fact_prices_ohlc_hourly')}}
    WHERE
        provider = 'coingecko'
       qualify ROW_NUMBER() over (
            PARTITION BY HOUR,
            asset_id
            ORDER BY
                modified_timestamp DESC
        ) = 1
),

-- Get best price for each token/blockchain/hour combination (THIRD priority - fallback)
prices AS (
    SELECT
        HOUR,
        blockchain,
        symbol,
        price,
        is_verified,
        is_imputed,
        -- Flag to track if this is a fallback price
        CASE
            WHEN blockchain = 'cosmos' THEN 'cosmos_fallback'
            WHEN blockchain = 'ethereum' THEN 'ethereum_stablecoin_fallback'
            ELSE 'direct_match'
        END AS price_source_type
    FROM
          {{ source('crosschain_price','ez_prices_hourly')}}
    qualify ROW_NUMBER() over (
            PARTITION BY HOUR,
            blockchain,
            symbol
            ORDER BY
                is_verified DESC,
                -- Prioritize verified prices
                is_imputed ASC,
                -- Prefer non-imputed
                inserted_timestamp DESC
        ) = 1
),

-- GMPs with interchain_transfers ARRAY - use flattened transfer data with embedded prices
gmp_with_interchain_transfers_array AS (
    SELECT
        f.id AS gmp_id,
        f.created_at,
        f.status,
        f.simplified_status,

        -- Source chain from the parent GMP call
        LOWER(f.call:chain::STRING) AS source_chain,

        -- Destination chain from the interchain_transfer object
        LOWER(it.value:destinationChain::STRING) AS destination_chain,

        -- Callback information
        LOWER(f.data:callback:chain::STRING) AS callback_chain,
        LOWER(f.data:callback:destinationAddress::STRING) AS callback_destination_address,

        -- Parent-child relationship
        f.call:parentMessageID::STRING AS parent_message_id,

        -- Token information
        it.value:symbol::STRING AS token_symbol,
        it.value:amount AS token_amount,
        it.value:value AS token_amount_usd,
        it.value:price AS token_price_usd,
        'interchain_transfers_embedded' AS price_source,

        -- Address information
        it.value:sourceAddress::STRING AS sender_address,
        it.value:destinationAddress::STRING AS destination_contract_address,
        it.value:recipient::STRING AS recipient_address,

        -- Transaction identifiers
        f.call:transactionHash::STRING AS source_tx_hash,
        f.call:event::STRING AS source_event,
        f.call:blockNumber::NUMBER AS source_block_number,

        -- Additional metadata
        it.value:contract_address::STRING AS token_contract_address,

        -- Source type flag
        'interchain_transfers' AS data_source,

        -- Generate unique ID for each flattened transfer
        {{ dbt_utils.generate_surrogate_key([
            'f.id',
            'it.index'
        ]) }} AS ez_gmp_activity_id,

        f.inserted_timestamp,
        SYSDATE() AS modified_timestamp

    FROM {{ ref('axelscan__fact_gmp') }} f,
    LATERAL FLATTEN(f.data:interchain_transfers) it
    WHERE f.data:interchain_transfers IS NOT NULL
        AND ARRAY_SIZE(f.data:interchain_transfers) > 0

{% if is_incremental() %}
    AND f.modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '3 DAYS'
        FROM {{ this }}
    )
{% endif %}
),

-- GMPs with interchain_transfer SINGLE OBJECT - needs price lookups
gmp_with_interchain_transfer_single_base AS (
    SELECT
        f.id AS gmp_id,
        f.created_at,
        f.status,
        f.simplified_status,

        -- Source chain from the parent GMP call
        LOWER(f.call:chain::STRING) AS source_chain,

        -- Destination chain from the interchain_transfer object (singular)
        LOWER(f.data:interchain_transfer:destinationChain::STRING) AS destination_chain,

        -- Callback information
        LOWER(f.data:callback:chain::STRING) AS callback_chain,
        LOWER(f.data:callback:destinationAddress::STRING) AS callback_destination_address,

        -- Parent-child relationship
        f.call:parentMessageID::STRING AS parent_message_id,

        -- Token information
        f.data:interchain_transfer:symbol::STRING AS token_symbol,
        f.data:interchain_transfer:amount AS token_amount,

        -- Address information
        f.data:interchain_transfer:sourceAddress::STRING AS sender_address,
        f.data:interchain_transfer:destinationAddress::STRING AS destination_contract_address,
        f.data:interchain_transfer:rawDestinationAddress::STRING AS recipient_address,

        -- Transaction identifiers
        f.call:transactionHash::STRING AS source_tx_hash,
        f.call:event::STRING AS source_event,
        f.call:blockNumber::NUMBER AS source_block_number,

        -- Additional metadata
        f.data:interchain_transfer:contract_address::STRING AS token_contract_address,

        -- Source type flag
        'interchain_transfer_single' AS data_source,

        -- Generate unique ID
        {{ dbt_utils.generate_surrogate_key([
            'f.id',
            "'single'"
        ]) }} AS ez_gmp_activity_id,

        f.inserted_timestamp,
        SYSDATE() AS modified_timestamp

    FROM {{ ref('axelscan__fact_gmp') }} f
    WHERE f.data:interchain_transfer IS NOT NULL
        AND f.data:interchain_transfer != '{}'

{% if is_incremental() %}
    AND f.modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '3 DAYS'
        FROM {{ this }}
    )
{% endif %}
),

-- Apply price lookups to singular interchain_transfers
gmp_with_interchain_transfer_single AS (
    SELECT
        g.gmp_id,
        g.created_at,
        g.status,
        g.simplified_status,
        g.source_chain,
        g.destination_chain,
        g.callback_chain,
        g.callback_destination_address,
        g.parent_message_id,
        g.token_symbol,
        g.token_amount,

        -- Calculate USD amount using price waterfall
        CASE
            WHEN g.token_amount IS NOT NULL
            AND COALESCE(
                p_coingecko.price,
                p_direct.price,
                p_xrp.price,
                p_native.price,
                p_wrapped_eth.price,
                p_cosmos.price,
                p_stablecoin.price
            ) IS NOT NULL THEN g.token_amount * COALESCE(
                p_coingecko.price,
                p_direct.price,
                p_xrp.price,
                p_native.price,
                p_wrapped_eth.price,
                p_cosmos.price,
                p_stablecoin.price
            )
            ELSE NULL
        END AS token_amount_usd,

        -- Get the price
        COALESCE(
            p_coingecko.price,
            p_direct.price,
            p_xrp.price,
            p_native.price,
            p_wrapped_eth.price,
            p_cosmos.price,
            p_stablecoin.price
        ) AS token_price_usd,

        -- Track price source
        CASE
            WHEN p_coingecko.price IS NOT NULL THEN 'coingecko_lookup'
            WHEN p_direct.price IS NOT NULL THEN 'direct'
            WHEN p_xrp.price IS NOT NULL THEN 'xrp_blockchain'
            WHEN p_native.price IS NOT NULL THEN 'native_unwrapped'
            WHEN p_wrapped_eth.price IS NOT NULL THEN 'ethereum_unwrapped'
            WHEN p_cosmos.price IS NOT NULL THEN 'cosmos_fallback'
            WHEN p_stablecoin.price IS NOT NULL THEN 'ethereum_stablecoin_fallback'
            ELSE NULL
        END AS price_source,

        g.sender_address,
        g.destination_contract_address,
        g.recipient_address,
        g.source_tx_hash,
        g.source_event,
        g.source_block_number,
        g.token_contract_address,
        g.data_source,
        g.ez_gmp_activity_id,
        g.inserted_timestamp,
        g.modified_timestamp

    FROM gmp_with_interchain_transfer_single_base g
    -- Join to token metadata to get coingecko_id
    LEFT JOIN token_meta tm
        ON g.token_symbol = tm.token_symbol
    -- Join to CoinGecko prices (FIRST priority for singular)
    LEFT JOIN coingecko_prices p_coingecko
        ON DATE_TRUNC('hour', g.created_at) = p_coingecko.hour
        AND tm.coingecko_id = p_coingecko.coingecko_id
    -- First try: Direct blockchain match
    LEFT JOIN prices p_direct
        ON DATE_TRUNC('hour', g.created_at) = p_direct.hour
        AND (
            g.token_symbol = p_direct.symbol
            OR (
                g.token_symbol LIKE 'axl%'
                AND REGEXP_REPLACE(g.token_symbol, '^axl', '') = p_direct.symbol
            )
        )
        AND CASE
            WHEN g.source_chain = 'binance' THEN 'bsc'
            WHEN g.source_chain = 'moonbeam' THEN 'moonbeam'
            WHEN g.source_chain = 'kava' THEN 'kava'
            ELSE g.source_chain
        END = p_direct.blockchain
    -- Second try: XRP on xrp blockchain
    LEFT JOIN prices p_xrp
        ON p_direct.price IS NULL
        AND DATE_TRUNC('hour', g.created_at) = p_xrp.hour
        AND g.token_symbol IN ('XRP', 'mXRP')
        AND p_xrp.symbol = 'XRP'
        AND p_xrp.blockchain = 'xrp'
    -- Third try: Unwrap Axelar tokens and match to native chain
    LEFT JOIN prices p_native
        ON p_direct.price IS NULL
        AND p_xrp.price IS NULL
        AND DATE_TRUNC('hour', g.created_at) = p_native.hour
        AND g.token_symbol LIKE 'axl%'
        AND REGEXP_REPLACE(g.token_symbol, '^axl', '') = p_native.symbol
        AND CASE
            WHEN REGEXP_REPLACE(g.token_symbol, '^axl', '') IN ('BNB', 'BUSD', 'BTCB') THEN p_native.blockchain = 'bsc'
            WHEN REGEXP_REPLACE(g.token_symbol, '^axl', '') IN ('MATIC', 'PolygonUSDC', 'PolygonUSDT') THEN p_native.blockchain = 'polygon'
            WHEN REGEXP_REPLACE(g.token_symbol, '^axl', '') IN ('AVAX', 'AvalancheUSDC') THEN p_native.blockchain = 'avalanche'
            WHEN REGEXP_REPLACE(g.token_symbol, '^axl', '') IN ('FTM') THEN p_native.blockchain = 'fantom'
            WHEN REGEXP_REPLACE(g.token_symbol, '^axl', '') IN ('OP', 'OptimismUSDT', 'OptimismUSDC') THEN p_native.blockchain = 'optimism'
            WHEN REGEXP_REPLACE(g.token_symbol, '^axl', '') IN ('ARB', 'ArbitrumUSDT', 'ArbitrumUSDC') THEN p_native.blockchain = 'arbitrum'
            WHEN REGEXP_REPLACE(g.token_symbol, '^axl', '') = 'REGEN' THEN p_native.blockchain = 'regen'
            WHEN REGEXP_REPLACE(g.token_symbol, '^axl', '') = 'DEUS' THEN p_native.blockchain = 'fantom'
            ELSE FALSE
        END
    -- Fourth try: Unwrap Axelar tokens and use Ethereum price
    LEFT JOIN prices p_wrapped_eth
        ON p_direct.price IS NULL
        AND p_xrp.price IS NULL
        AND p_native.price IS NULL
        AND DATE_TRUNC('hour', g.created_at) = p_wrapped_eth.hour
        AND g.token_symbol LIKE 'axl%'
        AND REGEXP_REPLACE(g.token_symbol, '^axl', '') = p_wrapped_eth.symbol
        AND p_wrapped_eth.blockchain = 'ethereum'
    -- Fifth try: Cosmos ecosystem fallback
    LEFT JOIN prices p_cosmos
        ON p_direct.price IS NULL
        AND p_xrp.price IS NULL
        AND p_native.price IS NULL
        AND p_wrapped_eth.price IS NULL
        AND DATE_TRUNC('hour', g.created_at) = p_cosmos.hour
        AND (
            g.token_symbol = p_cosmos.symbol
            OR (
                g.token_symbol LIKE 'axl%'
                AND REGEXP_REPLACE(g.token_symbol, '^axl', '') = p_cosmos.symbol
            )
        )
        AND p_cosmos.blockchain = 'cosmos'
        AND g.source_chain IN (
            'axelar', 'osmosis', 'cosmos', 'cosmoshub', 'neutron',
            'terra', 'terra-2', 'agoric', 'kujira', 'umee', 'stride',
            'regen', 'juno', 'evmos', 'crescent', 'kava', 'secret',
            'injective', 'sei', 'celestia', 'dydx'
        )
    -- Sixth try: Ethereum stablecoin fallback
    LEFT JOIN prices p_stablecoin
        ON p_direct.price IS NULL
        AND p_xrp.price IS NULL
        AND p_native.price IS NULL
        AND p_wrapped_eth.price IS NULL
        AND p_cosmos.price IS NULL
        AND DATE_TRUNC('hour', g.created_at) = p_stablecoin.hour
        AND (
            g.token_symbol = p_stablecoin.symbol
            OR (
                g.token_symbol LIKE 'axl%'
                AND REGEXP_REPLACE(g.token_symbol, '^axl', '') = p_stablecoin.symbol
            )
        )
        AND p_stablecoin.blockchain = 'ethereum'
        AND (
            p_stablecoin.symbol IN ('USDC', 'USDT', 'DAI', 'BUSD', 'FRAX', 'TUSD', 'USDP', 'GUSD', 'LUSD')
            OR REGEXP_REPLACE(g.token_symbol, '^axl', '') IN ('USDC', 'USDT', 'DAI', 'BUSD', 'FRAX', 'TUSD', 'USDP', 'GUSD', 'LUSD')
        )
),

-- GMPs without ANY interchain transfer data - build from fact_gmp with inline parsing
gmp_without_interchain_transfers_base AS (
    SELECT
        f.id AS gmp_id,
        f.created_at,
        f.status,
        f.simplified_status,

        -- Source chain from call
        TRIM(REPLACE(LOWER(f.call:chain::STRING), 'axelarnet', 'axelar')) AS source_chain,

        -- Destination chain from call returnValues
        TRIM(REPLACE(LOWER(f.call:returnValues:destinationChain::STRING), 'axelarnet', 'axelar')) AS destination_chain,

        -- Callback information
        LOWER(f.data:callback:chain::STRING) AS callback_chain,
        LOWER(f.data:callback:destinationAddress::STRING) AS callback_destination_address,

        -- Parent-child relationship
        f.call:parentMessageID::STRING AS parent_message_id,

        -- Token information (with fallback to DATA object for ContractCall events)
        COALESCE(
            f.call:returnValues:symbol::STRING,
            f.data:symbol::STRING
        ) AS token_symbol,

        COALESCE(
            f.amount,
            f.data:amount::FLOAT
        ) AS token_amount,

        -- Address information
        LOWER(f.call:transaction:from::STRING) AS sender_address,
        LOWER(f.call:returnValues:destinationContractAddress::STRING) AS destination_contract_address,
        NULL AS recipient_address,

        -- Transaction identifiers
        f.call:transactionHash::STRING AS source_tx_hash,
        f.call:event::STRING AS source_event,
        f.call:blockNumber::NUMBER AS source_block_number,

        -- Additional metadata
        NULL AS token_contract_address,

        -- Source type flag
        'standard_gmp' AS data_source,

        -- Generate unique ID
        {{ dbt_utils.generate_surrogate_key([
            'f.id'
        ]) }} AS ez_gmp_activity_id,

        f.inserted_timestamp,
        SYSDATE() AS modified_timestamp

    FROM {{ ref('axelscan__fact_gmp') }} f
    WHERE NOT (
        -- Exclude GMPs that have interchain_transfers ARRAY
        (f.data:interchain_transfers IS NOT NULL AND ARRAY_SIZE(f.data:interchain_transfers) > 0)
        OR
        -- Exclude GMPs that have interchain_transfer OBJECT
        (f.data:interchain_transfer IS NOT NULL AND f.data:interchain_transfer != '{}')
    )

{% if is_incremental() %}
    AND f.modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '3 DAYS'
        FROM {{ this }}
    )
{% endif %}
),

-- Apply price lookups to standard GMPs
gmp_without_interchain_transfers AS (
    SELECT
        g.gmp_id,
        g.created_at,
        g.status,
        g.simplified_status,
        g.source_chain,
        g.destination_chain,
        g.callback_chain,
        g.callback_destination_address,
        g.parent_message_id,
        g.token_symbol,
        g.token_amount,

        -- Calculate USD amount using price waterfall
        CASE
            WHEN g.token_amount IS NOT NULL
            AND COALESCE(
                p_coingecko.price,
                p_direct.price,
                p_xrp.price,
                p_native.price,
                p_wrapped_eth.price,
                p_cosmos.price,
                p_stablecoin.price
            ) IS NOT NULL THEN g.token_amount * COALESCE(
                p_coingecko.price,
                p_direct.price,
                p_xrp.price,
                p_native.price,
                p_wrapped_eth.price,
                p_cosmos.price,
                p_stablecoin.price
            )
            ELSE NULL
        END AS token_amount_usd,

        -- Get the price
        COALESCE(
            p_coingecko.price,
            p_direct.price,
            p_xrp.price,
            p_native.price,
            p_wrapped_eth.price,
            p_cosmos.price,
            p_stablecoin.price
        ) AS token_price_usd,

        -- Track price source
        CASE
            WHEN p_coingecko.price IS NOT NULL THEN 'coingecko_lookup'
            WHEN p_direct.price IS NOT NULL THEN 'direct'
            WHEN p_xrp.price IS NOT NULL THEN 'xrp_blockchain'
            WHEN p_native.price IS NOT NULL THEN 'native_unwrapped'
            WHEN p_wrapped_eth.price IS NOT NULL THEN 'ethereum_unwrapped'
            WHEN p_cosmos.price IS NOT NULL THEN 'cosmos_fallback'
            WHEN p_stablecoin.price IS NOT NULL THEN 'ethereum_stablecoin_fallback'
            ELSE NULL
        END AS price_source,

        g.sender_address,
        g.destination_contract_address,
        g.recipient_address,
        g.source_tx_hash,
        g.source_event,
        g.source_block_number,
        g.token_contract_address,
        g.data_source,
        g.ez_gmp_activity_id,
        g.inserted_timestamp,
        g.modified_timestamp

    FROM gmp_without_interchain_transfers_base g
    -- Join to token metadata to get coingecko_id
    LEFT JOIN token_meta tm
        ON g.token_symbol = tm.token_symbol
    -- Join to CoinGecko prices
    LEFT JOIN coingecko_prices p_coingecko
        ON DATE_TRUNC('hour', g.created_at) = p_coingecko.hour
        AND tm.coingecko_id = p_coingecko.coingecko_id
    -- Direct blockchain match
    LEFT JOIN prices p_direct
        ON DATE_TRUNC('hour', g.created_at) = p_direct.hour
        AND (
            g.token_symbol = p_direct.symbol
            OR (
                g.token_symbol LIKE 'axl%'
                AND REGEXP_REPLACE(g.token_symbol, '^axl', '') = p_direct.symbol
            )
        )
        AND CASE
            WHEN g.source_chain = 'binance' THEN 'bsc'
            WHEN g.source_chain = 'moonbeam' THEN 'moonbeam'
            WHEN g.source_chain = 'kava' THEN 'kava'
            ELSE g.source_chain
        END = p_direct.blockchain
    -- XRP fallback
    LEFT JOIN prices p_xrp
        ON p_direct.price IS NULL
        AND DATE_TRUNC('hour', g.created_at) = p_xrp.hour
        AND g.token_symbol IN ('XRP', 'mXRP')
        AND p_xrp.symbol = 'XRP'
        AND p_xrp.blockchain = 'xrp'
    -- Unwrap Axelar tokens - native chain
    LEFT JOIN prices p_native
        ON p_direct.price IS NULL
        AND p_xrp.price IS NULL
        AND DATE_TRUNC('hour', g.created_at) = p_native.hour
        AND g.token_symbol LIKE 'axl%'
        AND REGEXP_REPLACE(g.token_symbol, '^axl', '') = p_native.symbol
        AND CASE
            WHEN REGEXP_REPLACE(g.token_symbol, '^axl', '') IN ('BNB', 'BUSD', 'BTCB') THEN p_native.blockchain = 'bsc'
            WHEN REGEXP_REPLACE(g.token_symbol, '^axl', '') IN ('MATIC', 'PolygonUSDC', 'PolygonUSDT') THEN p_native.blockchain = 'polygon'
            WHEN REGEXP_REPLACE(g.token_symbol, '^axl', '') IN ('AVAX', 'AvalancheUSDC') THEN p_native.blockchain = 'avalanche'
            WHEN REGEXP_REPLACE(g.token_symbol, '^axl', '') IN ('FTM') THEN p_native.blockchain = 'fantom'
            WHEN REGEXP_REPLACE(g.token_symbol, '^axl', '') IN ('OP', 'OptimismUSDT', 'OptimismUSDC') THEN p_native.blockchain = 'optimism'
            WHEN REGEXP_REPLACE(g.token_symbol, '^axl', '') IN ('ARB', 'ArbitrumUSDT', 'ArbitrumUSDC') THEN p_native.blockchain = 'arbitrum'
            WHEN REGEXP_REPLACE(g.token_symbol, '^axl', '') = 'REGEN' THEN p_native.blockchain = 'regen'
            WHEN REGEXP_REPLACE(g.token_symbol, '^axl', '') = 'DEUS' THEN p_native.blockchain = 'fantom'
            ELSE FALSE
        END
    -- Unwrap Axelar tokens - Ethereum fallback
    LEFT JOIN prices p_wrapped_eth
        ON p_direct.price IS NULL
        AND p_xrp.price IS NULL
        AND p_native.price IS NULL
        AND DATE_TRUNC('hour', g.created_at) = p_wrapped_eth.hour
        AND g.token_symbol LIKE 'axl%'
        AND REGEXP_REPLACE(g.token_symbol, '^axl', '') = p_wrapped_eth.symbol
        AND p_wrapped_eth.blockchain = 'ethereum'
    -- Cosmos ecosystem fallback
    LEFT JOIN prices p_cosmos
        ON p_direct.price IS NULL
        AND p_xrp.price IS NULL
        AND p_native.price IS NULL
        AND p_wrapped_eth.price IS NULL
        AND DATE_TRUNC('hour', g.created_at) = p_cosmos.hour
        AND (
            g.token_symbol = p_cosmos.symbol
            OR (
                g.token_symbol LIKE 'axl%'
                AND REGEXP_REPLACE(g.token_symbol, '^axl', '') = p_cosmos.symbol
            )
        )
        AND p_cosmos.blockchain = 'cosmos'
        AND g.source_chain IN (
            'axelar', 'osmosis', 'cosmos', 'cosmoshub', 'neutron',
            'terra', 'terra-2', 'agoric', 'kujira', 'umee', 'stride',
            'regen', 'juno', 'evmos', 'crescent', 'kava', 'secret',
            'injective', 'sei', 'celestia', 'dydx'
        )
    -- Ethereum stablecoin fallback
    LEFT JOIN prices p_stablecoin
        ON p_direct.price IS NULL
        AND p_xrp.price IS NULL
        AND p_native.price IS NULL
        AND p_wrapped_eth.price IS NULL
        AND p_cosmos.price IS NULL
        AND DATE_TRUNC('hour', g.created_at) = p_stablecoin.hour
        AND (
            g.token_symbol = p_stablecoin.symbol
            OR (
                g.token_symbol LIKE 'axl%'
                AND REGEXP_REPLACE(g.token_symbol, '^axl', '') = p_stablecoin.symbol
            )
        )
        AND p_stablecoin.blockchain = 'ethereum'
        AND (
            p_stablecoin.symbol IN ('USDC', 'USDT', 'DAI', 'BUSD', 'FRAX', 'TUSD', 'USDP', 'GUSD', 'LUSD')
            OR REGEXP_REPLACE(g.token_symbol, '^axl', '') IN ('USDC', 'USDT', 'DAI', 'BUSD', 'FRAX', 'TUSD', 'USDP', 'GUSD', 'LUSD')
        )
),

-- Combine all three sources
combined AS (
    SELECT * FROM gmp_with_interchain_transfers_array
    UNION ALL
    SELECT * FROM gmp_with_interchain_transfer_single
    UNION ALL
    SELECT * FROM gmp_without_interchain_transfers
)

SELECT
    -- Core identifiers
    gmp_id,
    ez_gmp_activity_id,

    -- Timestamps
    created_at,

    -- Status
    status,
    simplified_status,

    -- Chain routing
    source_chain,
    destination_chain,
    callback_chain,
    callback_destination_address,

    -- Parent-child relationships
    parent_message_id,

    -- Token and amounts
    token_symbol,
    token_amount,
    token_amount_usd,
    token_price_usd,
    price_source,

    -- Addresses
    sender_address,
    destination_contract_address,
    recipient_address,
    token_contract_address,

    -- Transaction details
    source_tx_hash,
    source_event,
    source_block_number,

    -- Metadata
    data_source,

    -- System timestamps
    inserted_timestamp,
    modified_timestamp
FROM combined
WHERE parent_message_id IS NULL  -- Exclude child GMPs to match metrics counting methodology
    AND source_chain IS NOT NULL  -- Exclude records with missing chain data (87 historical records)
    AND destination_chain IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ez_gmp_activity_id
    ORDER BY modified_timestamp DESC
) = 1
