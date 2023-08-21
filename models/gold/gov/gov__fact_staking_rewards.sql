{{ config(
    materialized = 'view',
      meta={
        'database_tags':{
            'table': {
                'PURPOSE': 'STAKING'
            }
        }
      }
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    tx_caller_address,
    action,
    delegator_address,
    amount,
    currency,
    validator_address,
    msg_group AS _msg_group
FROM
    {{ ref('silver__staking_rewards') }}
