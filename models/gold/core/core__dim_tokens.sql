{{ config(
    materialized = 'view',
    tags = ['noncore']
) }}

SELECT
    'axelar' AS blockchain,
    address,
    creator,
    label_type,
    label_subtype,
    label,
    project_name,
    alias,
    DECIMAL,
    raw_metadata,
    unique_key,
    unique_key AS dim_tokens_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref(
        'silver__osmo_assets'
    ) }}
