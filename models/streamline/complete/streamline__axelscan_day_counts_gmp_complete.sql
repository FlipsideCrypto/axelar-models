-- depends_on: {{ ref('bronze__axelscan_day_counts_gmp') }}
{{ config (
    materialized = "incremental",
    unique_key = 'date_day',
) }}

SELECT
    (LEFT(partition_key, 4) || '-' || SUBSTRING(partition_key, 5, 2) || '-' || RIGHT(partition_key, 2)) :: DATE date_day,
    VALUE :FROMTIME :: bigint AS fromTime,
    VALUE :TOTIME :: bigint AS toTime,
    DATA :total AS day_count,
    {{ dbt_utils.generate_surrogate_key(
        ['date_day']
    ) }} AS axelscan_day_counts_gmp_complete_ID,
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

        qualify(ROW_NUMBER() over (PARTITION BY date_day
        ORDER BY
            inserted_timestamp DESC)) = 1
