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
    NULL AS raw_metadata
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
    raw_metadata
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
    raw_metadata
FROM
    {{ ref('gov__fact_validators') }}
