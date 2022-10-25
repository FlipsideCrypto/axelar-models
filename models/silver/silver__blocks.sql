{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

SELECT
    block_id,
    COALESCE(
        DATA :result :block :header :time :: TIMESTAMP,
        DATA :block :header :time :: TIMESTAMP,
        DATA :result :block :header :timestamp :: TIMESTAMP,
        DATA :block :header :timestamp :: TIMESTAMP
    ) AS block_timestamp,
    COALESCE(
        DATA :result :block :header :chain_id :: STRING,
        DATA :block :header :chain_id :: STRING
    ) AS chain_id,
    COALESCE(
        ARRAY_SIZE(
            DATA :result :block :data :txs
        ) :: NUMBER,
        ARRAY_SIZE(
            DATA :block :data :txs
        ) :: NUMBER
    ) AS tx_count,
    COALESCE(
        DATA :result :block :header :proposer_address :: STRING,
        DATA :block :header :proposer_address :: STRING
    ) AS proposer_address,
    COALESCE(
        DATA :result :block :header :validators_hash :: STRING,
        DATA :block :header :validators_hash :: STRING
    ) AS validator_hash,
    COALESCE(
        DATA :result :block :header,
        DATA :block :header
    ) AS header,
    _partition_by_block_id,
    concat_ws(
        '-',
        chain_id,
        block_id
    ) AS _unique_key
FROM
    {{ ref('bronze__blocks') }}
WHERE
    VALUE :data :error IS NULL

{% if is_incremental() %}
AND _partition_by_block_id >= (
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
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY chain_id, block_id
ORDER BY
    _partition_by_block_id DESC)) = 1
