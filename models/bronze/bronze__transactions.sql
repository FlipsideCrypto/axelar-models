{{ config(
    materialized = 'incremental',
    unique_key = 'tx_id',
    cluster_by = ['_inserted_timestamp::date'],
    merge_update_columns = ["tx_id"],
) }}

WITH meta AS (

    SELECT
        registered_on,
        last_modified,
        LEAST(
            last_modified,
            registered_on
        ) AS _inserted_timestamp,
        file_name
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze", "txs_details") }}'
            )
        ) A

{% if is_incremental() %}
WHERE
    LEAST(
        registered_on,
        last_modified
    ) >= (
        SELECT
            COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
        FROM
            {{ this }})
    ),
    partitions AS (
        SELECT
            DISTINCT TO_DATE(
                concat_ws('-', SPLIT_PART(file_name, '/', 3), SPLIT_PART(file_name, '/', 4), SPLIT_PART(file_name, '/', 5))
            ) AS _partition_by_modified_date
        FROM
            meta
    )
{% else %}
)
{% endif %}

SELECT
    VALUE,
    _partition_by_block_id,
    block_number AS block_id,
    metadata,
    DATA,
    tx_hash :: STRING AS tx_id, 
    tx_result,
    TO_TIMESTAMP(
        m._inserted_timestamp
    ) AS _inserted_timestamp
FROM
    {{ source(
        'bronze',
        'txs_details'
    ) }}
    JOIN meta m
    ON m.file_name = metadata$filename
WHERE
    DATA: error IS NULL

{% if is_incremental() %}
JOIN partitions p
ON p._partition_by_modified_date = s._partition_by_modified_date
{% endif %}
