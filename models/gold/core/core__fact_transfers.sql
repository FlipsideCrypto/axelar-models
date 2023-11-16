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
    msg_index,
    COALESCE(
        transfers_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','msg_index']
        ) }}
    ) AS fact_transfers_id,
    inserted_timestamp,
    modified_timestamp
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
    msg_index,
    COALESCE(
        transfers_ibc_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','msg_index']
        ) }}
    ) AS fact_transfers_id,
    inserted_timestamp,
    modified_timestamp
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
    msg_index,
    COALESCE(
        transfer_executependingtransfers_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','msg_index']
        ) }}
    ) AS fact_transfers_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__transfers_ExecutePendingTransfers') }}
