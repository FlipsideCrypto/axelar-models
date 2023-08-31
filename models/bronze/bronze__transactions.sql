{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'file_name',
    cluster_by = ['_inserted_timestamp::date']
) }}

WITH meta AS (

    SELECT
        registered_on,
        last_modified,
        GREATEST(
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
    GREATEST(
        registered_on,
        last_modified
    ) >= DATEADD(
        DAY,
        -2,(
            SELECT
                COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
            FROM
                {{ this }})
        )
    )
{% else %}
)
{% endif %}
SELECT
    VALUE,
    _partition_by_block_id,
    block_number AS block_id,
    REPLACE(
        metadata :request :params [0],
        'tx.height='
    ) :: INT AS block_id_requested,
    metadata,
    DATA,
    tx_hash :: STRING AS tx_id,
    tx_result,
    file_name,
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
