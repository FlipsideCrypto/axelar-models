{{ config(
    materialized = 'incremental',
    unique_key = "address",
    incremental_strategy = 'merge',
    cluster_by = 'system_created_at::DATE'
) }}

SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    label_type,
    label_subtype,
    address_name,
    project_name
FROM
    {{ source(
        'crosschain',
        'dim_labels'
    ) }}
WHERE
    blockchain = 'axelar'

{% if is_incremental() %}
AND system_created_at :: DATE >= (
    SELECT
        MAX(system_created_at) :: DATE -2
    FROM
        {{ this }}
)
{% endif %}
