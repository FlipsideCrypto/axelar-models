{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = 'block_timestamp::DATE',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
) }}

WITH base_table AS (

    SELECT
        block_id,
        DATA: HASH :: STRING AS tx_id,
        'axelar' AS blockchain,
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
        _partition_by_block_id
    FROM
        {{ ref('bronze__transactions') }}
    WHERE
        DATA :error IS NULL

{% if is_incremental() %}
AND _partition_by_block_id >= (
    SELECT
        MAX(_partition_by_block_id) - 2000
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    b.block_id,
    block_timestamp,
    tx_id,
    blockchain,
    chain_id,
    codespace,
    gas_used,
    gas_wanted,
    tx_succeeded,
    tx_code,
    msgs,
    tx_log :: STRING AS tx_log,
    b._partition_by_block_id
FROM
    base_table b
    LEFT OUTER JOIN {{ ref('silver__blocks') }}
    bb
    ON b.block_id = bb.block_id

{% if is_incremental() %}
WHERE
    b._partition_by_block_id >= (
        SELECT
            MAX(_partition_by_block_id) - 2000
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY tx_id
ORDER BY
    b._partition_by_block_id DESC)) = 1
