{{ config(
    materialized = 'view',
    tags = ['daily']
) }}

SELECT
    id,
    created_at,
    status,
    amount,
    destination_chain_type,
    event,
    simplified_status,
    call,
    command_id,
    fees,
    gas_status,
    is_call_from_relayer,
    is_invalid_call,
    is_invalid_destination_chain,
    is_two_way,
    time_spent,
    DATA,
    axelscan_gmp_id AS fact_gmp_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__axelscan_gmp') }}
