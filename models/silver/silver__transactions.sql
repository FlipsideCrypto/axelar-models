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
        d.value: hash :: STRING AS tx_id,
        'axelar' AS blockchain,
        d.value :tx_result :codespace AS codespace,
        d.value :tx_result :gas_used :: NUMBER AS gas_used,
        d.value :tx_result :gas_wanted :: NUMBER AS gas_wanted,
        CASE
            WHEN d.value :tx_result :code :: NUMBER = 0 THEN TRUE
            ELSE FALSE
        END AS tx_succeeded,
        d.value :tx_result :code :: NUMBER AS tx_code,
        d.value :tx_result :events AS msgs,
        d.value :tx_result :log AS tx_log,
        _partition_by_block_id
    FROM
        {{ ref('bronze__transactions') }}
        t,
        TABLE(FLATTEN(DATA :result :txs)) d

{% if is_incremental() %}
WHERE
    _partition_by_block_id >= (
        SELECT
            MAX(_partition_by_block_id) -1
        FROM
            {{ this }}
    )
    AND _partition_by_block_id <= (
        SELECT
            MAX(_partition_by_block_id) + 10
        FROM
            {{ this }}
    )
{% else %}
WHERE
    _partition_by_block_id IN (
        0,
        1
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
    bb._partition_by_block_id >= (
        SELECT
            MAX(_partition_by_block_id) -1
        FROM
            {{ this }}
    )
    AND bb._partition_by_block_id <= (
        SELECT
            MAX(_partition_by_block_id) + 10
        FROM
            {{ this }}
    )
{% else %}
WHERE
    bb._partition_by_block_id IN (
        0,
        1
    )
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY tx_id
ORDER BY
    b._partition_by_block_id DESC)) = 1
