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
    voter,
    proposal_id,
    vote_option,
    vote_weight,
    COALESCE(
        governance_votes_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','proposal_id','voter']
        ) }}
    ) AS fact_governance_votes_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__governance_votes') }}
