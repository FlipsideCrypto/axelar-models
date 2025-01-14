-- depends_on: {{ ref('bronze__axelscan_day_counts_gmp') }}
{{ config (
    materialized = "incremental",
    unique_key = ['date_day','from_time'],
    tags = ['streamline_axelscan']
) }}

SELECT
    partition_key AS date_day,
    VALUE :FROM_TIME :: bigint AS from_time,
    VALUE :TO_TIME :: bigint AS TO_TIME,
    DATA :total AS day_count,
    {{ dbt_utils.generate_surrogate_key(
        ['date_day','from_time']
    ) }} AS axelscan_day_counts_gmp_complete_id,
    inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__axelscan_day_counts_gmp') }}
WHERE
    inserted_timestamp >= (
        SELECT
            COALESCE(MAX(inserted_timestamp), '1970-01-01' :: DATE) inserted_timestamp
        FROM
            {{ this }})
        {% else %}
            {{ ref('bronze__axelscan_day_counts_gmp_FR') }}
        {% endif %}

        qualify(ROW_NUMBER() over (PARTITION BY date_day, from_time
        ORDER BY
            inserted_timestamp DESC)) = 1
