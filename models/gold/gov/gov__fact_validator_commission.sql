{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STAKING' }} },
    tags = ['noncore']
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    tx_caller_address,
    msg_group,
    amount,
    currency,
    validator_address_operator,
    validator_address_reward,
    COALESCE(
        validator_commission_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id']
        ) }}
    ) AS fact_validator_commission_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__validator_commission') }}
