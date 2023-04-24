{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}
-- depends_on: {{ ref('streamline__blocks_history') }}
-- depends_on: {{ ref('streamline__blocks_history_FR') }}

SELECT
    block_number AS block_id,
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
    TO_TIMESTAMP(
        _inserted_timestamp
    ) AS _inserted_timestamp,
    concat_ws(
        '-',
        chain_id,
        block_id
    ) AS _unique_key
FROM

{% if is_incremental() %}
{{ ref('streamline__blocks_history') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('streamline__blocks_history_FR') }}
WHERE
    1 = 1
{% endif %}
AND VALUE :data :error IS NULL
AND DATA :error IS NULL qualify(ROW_NUMBER() over (PARTITION BY block_number
ORDER BY
    _inserted_timestamp DESC)) = 1
