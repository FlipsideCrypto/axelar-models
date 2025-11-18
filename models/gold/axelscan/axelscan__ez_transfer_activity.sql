{{ config(
    materialized = 'incremental',
    unique_key = 'ez_transfer_activity_id',
    incremental_strategy = 'delete+insert',
    cluster_by = 'created_at::DATE',
    tags = ['daily']
) }}

SELECT
    -- Transaction identifiers
    id AS transfer_id,
    created_at,
    status,
    simplified_status,

    -- Source chain info
    TRIM(REPLACE(LOWER(source_chain), 'axelarnet', 'axelar')) AS source_chain,
    LOWER(sender_address) AS sender_address,

    -- Destination chain info
    TRIM(REPLACE(LOWER(destination_chain), 'axelarnet', 'axelar')) AS destination_chain,
    LOWER(recipient_address) AS recipient_address,

    -- Token and amount details
    send_denom AS token_denom,
    send_amount AS amount,
    send_amount_received AS amount_received,
    send_fee AS fee,

    -- Additional transaction details from nested send object
    send:txhash::STRING AS source_tx_hash,
    send:height::NUMBER AS source_height,
    send:type::STRING AS transfer_type,
    send:insufficient_fee::BOOLEAN AS insufficient_fee,
    send:value::FLOAT AS amount_value_usd,
    send:fee_value::FLOAT AS fee_value_usd,

    -- Confirmation details from nested confirm object
    DATA:confirm:txhash::STRING AS confirm_tx_hash,
    DATA:confirm:height::NUMBER AS confirm_height,
    LOWER(DATA:confirm:deposit_address::STRING) AS deposit_address,
    DATA:confirm:transfer_id::NUMBER AS axelar_transfer_id,

    -- IBC send details from nested ibc_send object
    DATA:ibc_send:ack_txhash::STRING AS ibc_ack_tx_hash,
    DATA:ibc_send:failed_txhash::STRING AS ibc_failed_tx_hash,
    DATA:ibc_send:height::NUMBER AS ibc_height,
    DATA:ibc_send:packet:packet_sequence::NUMBER AS ibc_packet_sequence,
    DATA:ibc_send:packet:packet_src_channel::STRING AS ibc_src_channel,
    DATA:ibc_send:packet:packet_dst_channel::STRING AS ibc_dst_channel,

    -- Link information
    link,

    -- Full nested objects for reference
    send,
    DATA:confirm AS confirm,
    DATA:ibc_send AS ibc_send,

    -- Generated fields
    {{ dbt_utils.generate_surrogate_key(
        ['id']
    ) }} AS ez_transfer_activity_id,
    fact_transfers_id,
    sysdate() inserted_timestamp,
    sysdate() modified_timestamp

FROM
    {{ ref('axelscan__fact_transfers') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
