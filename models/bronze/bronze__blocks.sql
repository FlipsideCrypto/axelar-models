{{ config(
    materialized = 'incremental',
    unique_key = 'block_id',
    cluster_by = ['_inserted_timestamp::date'],
    merge_update_columns = ["block_id"],
) }}
-- depends_on: {{ ref('bronze__streamline_blocks') }}
-- depends_on: {{ ref('bronze__streamline_FR_blocks') }}

SELECT
    VALUE,
    _partition_by_block_id,
    block_number AS block_id,
    metadata,
    DATA,
    TO_TIMESTAMP(
        m._inserted_timestamp
    ) AS _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_blocks') }}
{% else %}
    {{ ref('bronze__streamline_FR_blocks') }}
{% endif %}

m

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number
ORDER BY
    _inserted_timestamp DESC)) = 1
