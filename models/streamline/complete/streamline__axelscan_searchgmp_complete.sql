-- depends_on: {{ ref('bronze__axelscan_searchgmp') }}
{{ config (
    materialized = "incremental",
    unique_key = 'axelscan_searchgmp_complete_id',
    cluster_by = "date_day",
    tags = ['streamline_axelscan']
) }}

SELECT
    partition_key AS date_day,
    VALUE :ID :: INT AS id,
    {{ dbt_utils.generate_surrogate_key(
        ['date_day','id']
    ) }} AS axelscan_searchgmp_complete_id,
    inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__axelscan_searchgmp') }}
WHERE
    inserted_timestamp >= (
        SELECT
            COALESCE(MAX(inserted_timestamp), '1970-01-01' :: DATE) inserted_timestamp
        FROM
            {{ this }})
        {% else %}
            {{ ref('bronze__axelscan_searchgmp_FR') }}
        {% endif %}

        qualify(ROW_NUMBER() over (PARTITION BY axelscan_searchgmp_complete_id
        ORDER BY
            inserted_timestamp DESC)) = 1
