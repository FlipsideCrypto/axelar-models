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
    proposer,
    proposal_id,
    proposal_type,
    COALESCE(
        governance_submit_proposal_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id']
        ) }}
    ) AS fact_governance_submit_proposal_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__governance_submit_proposal') }}
