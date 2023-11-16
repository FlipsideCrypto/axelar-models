{{ config(
    materialized = 'view'
) }}

SELECT
    block_id,
    block_timestamp,
    chain_id,
    tx_count,
    proposer_address,
    validator_hash,
    COALESCE (
        blocks_id,
        {{ dbt_utils.generate_surrogate_key(
            ['chain_id','block_id']
        ) }}
    ) AS fact_blocks_id
FROM
    {{ ref('silver__blocks') }}
