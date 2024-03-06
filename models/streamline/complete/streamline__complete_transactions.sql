-- depends_on: {{ ref('bronze__streamline_transactions') }}
{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(id)"
) }}

SELECT
    id,
    block_number,
    metadata :request [2] AS page,
    _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_transactions') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
        FROM
            {{ this }})
    )
{% else %}
    {{ ref('bronze__streamline_FR_transactions') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
