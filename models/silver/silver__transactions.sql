{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = 'block_timestamp::DATE',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
) }}
-- depends_on: {{ ref('streamline__txs_history') }}
-- depends_on: {{ ref('streamline__txs_history_FR') }}
WITH base_table AS (

    SELECT
        block_number AS block_id,
        DATA :hash :: STRING AS tx_id,
        DATA :tx_result :codespace AS codespace,
        DATA :tx_result :gas_used :: NUMBER AS gas_used,
        DATA :tx_result :gas_wanted :: NUMBER AS gas_wanted,
        CASE
            WHEN DATA :tx_result :code :: NUMBER = 0 THEN TRUE
            ELSE FALSE
        END AS tx_succeeded,
        DATA :tx_result :code :: NUMBER AS tx_code,
        DATA :tx_result :events AS msgs,
        DATA :tx_result :log AS tx_log,
        TO_TIMESTAMP(
            _inserted_timestamp
        ) AS _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('streamline__txs_history') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('streamline__txs_history_FR') }}
WHERE
    1 = 1
{% endif %}
AND DATA :error IS NULL qualify(ROW_NUMBER() over (PARTITION BY DATA :hash :: STRING
ORDER BY
    _inserted_timestamp DESC)) = 1
)
SELECT
    b.block_id,
    block_timestamp,
    tx_id,
    codespace,
    gas_used,
    gas_wanted,
    tx_succeeded,
    tx_code,
    msgs,
    tx_log :: STRING AS tx_log,
    b._inserted_timestamp
FROM
    base_table b
    LEFT OUTER JOIN {{ ref('silver__blocks') }}
    bb
    ON b.block_id = bb.block_id

{% if is_incremental() %}
WHERE
    bb._inserted_timestamp :: DATE >= (
        SELECT
            MAX(_inserted_timestamp) :: DATE - 2
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY tx_id
ORDER BY
    b._inserted_timestamp DESC)) = 1
