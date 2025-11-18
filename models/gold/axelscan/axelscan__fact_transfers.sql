{{ config(
    materialized = 'view',
    tags = ['daily']
) }}

SELECT
    id,
    created_at,
    status,
    send_amount,
    send_amount_received,
    send_fee,
    send_denom,
    source_chain,
    destination_chain,
    sender_address,
    recipient_address,
    simplified_status,
    link,
    send,
    DATA,
    axelscan_transfers_id AS fact_transfers_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__axelscan_transfers') }}
