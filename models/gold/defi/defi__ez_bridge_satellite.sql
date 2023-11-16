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
    fee_denom
FROM
    {{ ref('silver__satellite_combined') }}
