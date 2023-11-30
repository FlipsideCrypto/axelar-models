{{ config(
    materialized = 'view',
    tags = ['noncore']
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    sender,
    token_address,
    token_symbol,
    amount,
    source_chain,
    destination_chain,
    receiver,
    COALESCE(
        squid_combined_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash']
        ) }}
    ) AS ez_bridge_squid_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__squid_combined') }}
