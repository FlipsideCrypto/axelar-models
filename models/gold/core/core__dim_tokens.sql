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
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref(
        'silver__osmo_assets'
    ) }}
