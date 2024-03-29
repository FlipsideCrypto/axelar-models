{{ config(
    materialized = 'view',
    tags = ['noncore']
) }}

SELECT
    address,
    blockchain,
    creator,
    label_type,
    label_subtype,
    label,
    project_name,
    delegator_shares,
    jailed,
    rate,
    max_change_rate,
    max_rate,
    min_self_delegation,
    RANK,
    raw_metadata,
    unique_key,
    COALESCE(
        validators_id,
        {{ dbt_utils.generate_surrogate_key(
            ['address','creator','blockchain']
        ) }}
    ) AS fact_validators_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__validators') }}
