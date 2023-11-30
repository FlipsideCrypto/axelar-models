{{ config(
    materialized = 'table',
    tags = ['daily']
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
    concat_ws(
        '-',
        address,
        creator,
        blockchain
    ) AS unique_key,
    {{ dbt_utils.generate_surrogate_key(
        ['address','creator','blockchain']
    ) }} AS osmos_assets_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ source(
        'osmo',
        'asset_metadata'
    ) }}
