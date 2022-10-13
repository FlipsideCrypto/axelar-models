{{ config(
    materialized = 'view'
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
    {{ source(
        'crosschain',
        'address_labels'
    ) }}
WHERE
    blockchain = 'axelar'
UNION ALL
SELECT
    'axelar',
    creator,
    address,
    label_type,
    label_subtype,
    label,
    project_name,
    raw_metadata
FROM
    {{ source(
        'tokens',
        'dim_labels'
    ) }}
WHERE
    label_type = 'token'
    AND label_subtype = 'token_contract'
