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
    t.decimal,
    COALESCE(
        governance_proposal_deposits_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id']
        ) }}
    ) AS fact_governance_proposal_deposits_id,
    GREATEST(
        g.inserted_timestamp,
        t.inserted_timestamp
    ) AS inserted_timestamp,
    GREATEST(
        g.modified_timestamp,
        t.modified_timestamp
    ) AS modified_timestamp
FROM
    {{ ref('silver__governance_proposal_deposits') }}
    g
    LEFT OUTER JOIN {{ ref('core__dim_tokens') }}
    t
    ON g.currency = t.address
