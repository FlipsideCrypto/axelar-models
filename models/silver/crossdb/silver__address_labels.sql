{{ config(
    materialized = 'incremental',
    unique_key = "address",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = 'system_created_at::DATE',
    tags = ['daily']
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
    project_name,
    {{ dbt_utils.generate_surrogate_key(
        ['address']
    ) }} AS address_labels_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
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
