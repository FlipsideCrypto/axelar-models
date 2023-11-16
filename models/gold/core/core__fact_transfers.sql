{{ config(
    materialized = 'view',
    tags = ['noncore']
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    transfer_type,
    sender,
    amount,
    currency,
    DECIMAL,
    receiver,
    foreign_address,
    foreign_chain,
    msg_index
FROM
    {{ ref('silver__transfers') }}
UNION ALL
SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    transfer_type,
    sender,
    amount,
    currency,
    DECIMAL,
    receiver,
    foreign_address,
    foreign_chain,
    msg_index
FROM
    {{ ref('silver__transfers_ibc') }}
UNION ALL
SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    transfer_type,
    sender,
    amount,
    currency,
    DECIMAL,
    receiver,
    foreign_address,
    foreign_chain,
    msg_index
FROM
    {{ ref('silver__transfers_ExecutePendingTransfers') }}
