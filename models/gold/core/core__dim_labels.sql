{{ config(
    materialized = 'view',
    tags = ['noncore']
) }}

SELECT
    blockchain,
    creator,
    address,
    label_type,
    label_subtype,
    project_name AS label,
    address_name AS address_name,
    NULL AS raw_metadata,
    {{ dbt_utils.generate_surrogate_key(
        [' address ']
    ) }} AS dim_labels_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref(
        'silver__address_labels'
    ) }}
UNION ALL
SELECT
    'axelar' AS blockchain,
    creator,
    address,
    label_type,
    label_subtype,
    label,
    project_name,
    raw_metadata,
    dim_tokens_id AS dim_labels_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('core__dim_tokens') }}
UNION ALL
SELECT
    'axelar' AS blockchain,
    creator,
    address,
    label_type,
    label_subtype,
    label,
    project_name,
    raw_metadata,
    fact_validators_id AS dim_labels_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('gov__fact_validators') }}
