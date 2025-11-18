{{ config(
    materialized = 'view',
    tags = ['daily']
) }}

-- Union of GMP activity and Transfer activity for a unified bridge activity view
SELECT
    'gmp' AS activity_type,
    gmp_id AS transaction_id,
    created_at,
    status,
    simplified_status,

    -- Chain and address info
    source_chain,
    destination_chain,
    sender_address,
    destination_contract_address,
    recipient_address,

    -- Token and amount info
    token_symbol,
    token_amount AS amount,
    token_amount_usd AS amount_value_usd,
    token_price_usd,

    -- Transaction details
    source_tx_hash,
    source_event AS event_type,
    source_block_number,

    -- Transfer-specific fields (NULL for GMP)
    NULL AS transfer_type,
    NULL AS amount_received,
    NULL AS insufficient_fee,
    NULL AS confirm_tx_hash,
    NULL AS deposit_address,
    NULL AS fee,
    NULL AS fee_value_usd,

    -- GMP-specific fields
    callback_chain,
    callback_destination_address,
    parent_message_id,
    token_contract_address,
    data_source,
    price_source,

    -- Metadata
    ez_gmp_activity_id AS ez_bridge_activity_id,
    inserted_timestamp,
    modified_timestamp

FROM {{ ref('axelscan__ez_gmp_activity') }}

UNION ALL

SELECT
    'transfer' AS activity_type,
    transfer_id AS transaction_id,
    created_at,
    status,
    simplified_status,

    -- Chain and address info
    source_chain,
    destination_chain,
    sender_address,
    NULL AS destination_contract_address,
    recipient_address,

    -- Token and amount info
    token_denom AS token_symbol,
    amount,
    amount_value_usd,
    NULL AS token_price_usd,

    -- Transaction details
    source_tx_hash,
    transfer_type AS event_type,
    source_height AS source_block_number,

    -- Transfer-specific fields
    transfer_type,
    amount_received,
    insufficient_fee,
    confirm_tx_hash,
    deposit_address,
    fee,
    fee_value_usd,

    -- GMP-specific fields (NULL for transfers)
    NULL AS callback_chain,
    NULL AS callback_destination_address,
    NULL AS parent_message_id,
    NULL AS token_contract_address,
    NULL AS data_source,
    NULL AS price_source,

    -- Metadata
    ez_transfer_activity_id AS ez_bridge_activity_id,
    inserted_timestamp,
    modified_timestamp

FROM {{ ref('axelscan__ez_transfer_activity') }}
