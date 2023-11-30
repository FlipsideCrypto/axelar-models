{{ config(
    materialized = 'view',
    tags = ['noncore']
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    source_chain,
    sender,
    destination_chain,
    receiver,
    token_address,
    token_symbol,
    amount,
    raw_amount,
    deposit_address,
    transfer_id,
    amount_received,
    amount_received_denom,
    fee_paid,
    fee_denom,
    COALESCE(
        satellite_combined_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash']
        ) }}
    ) AS ez_bridge_satellite_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__satellite_combined') }}
