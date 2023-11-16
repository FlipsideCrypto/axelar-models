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
    receiver
FROM
    {{ ref('silver__squid_combined') }}
