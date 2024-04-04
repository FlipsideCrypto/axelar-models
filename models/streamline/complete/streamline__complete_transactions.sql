-- depends_on: {{ ref('bronze__streamline_transactions') }}
{{ config (
    materialized = "incremental",
    unique_key = 'complete_transactions_id',
    cluster_by = "ROUND(block_number, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)"
) }}

SELECT
    DATA :height :: INT AS block_number,
    COALESCE(
        metadata :request :data :params [2],
        metadata :request :params [2]
    ) :: INT AS page,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number','page']
    ) }} AS complete_transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_transactions') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
        FROM
            {{ this }})
        {% else %}
            {{ ref('bronze__streamline_FR_transactions') }}
        {% endif %}

        qualify(ROW_NUMBER() over (PARTITION BY complete_transactions_id
        ORDER BY
            _inserted_timestamp DESC)) = 1
