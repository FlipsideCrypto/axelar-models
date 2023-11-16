{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'GOVERNANCE' }} },
    tags = ['noncore']
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    depositor,
    proposal_id,
    amount / pow(10, COALESCE(t.decimal, 0)) :: NUMBER AS amount,
    currency,
    t.decimal
FROM
    {{ ref('silver__governance_proposal_deposits') }}
    g
    LEFT OUTER JOIN {{ ref('core__dim_tokens') }}
    t
    ON g.currency = t.address
