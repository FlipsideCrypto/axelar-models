{{ config(
    materialized = 'view'
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
    unique_key
FROM
    {{ ref(
        'silver__osmo_assets'
    ) }}
