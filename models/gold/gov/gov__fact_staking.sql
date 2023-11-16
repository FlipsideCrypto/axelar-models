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
    action,
    delegator_address,
    validator_address,
    amount,
    currency,
    redelegate_source_validator_address,
    completion_time,
    msg_group AS _msg_group,
    COALESCE(
        staking_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','msg_group','action','currency','delegator_address','validator_address']
        ) }}
    ) AS fact_staking_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__staking') }}
