{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = 'block_timestamp::DATE',
    tags = ['core']
) }}

WITH base_atts AS (

    SELECT
        block_id,
        block_timestamp,
        tx_succeeded,
        tx_id,
        attribute_key,
        attribute_value,
        _inserted_timestamp
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type = 'link'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
fin AS (
    SELECT
        block_id,
        block_timestamp,
        tx_succeeded,
        tx_id,
        _inserted_timestamp,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :module :: STRING AS module,
        COALESCE(
            j :sourceChain :: STRING,
            j :Chain :: STRING
        ) AS source_chain,
        j :depositAddress :: STRING AS deposit_address,
        j :destinationChain :: STRING AS destination_chain,
        j :destinationAddress :: STRING AS destination_address,
        COALESCE(
            j :asset :: STRING,
            j :tokenAddress :: STRING
        ) AS asset
    FROM
        base_atts
    GROUP BY
        block_id,
        block_timestamp,
        tx_succeeded,
        tx_id,
        _inserted_timestamp
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    module,
    source_chain,
    deposit_address,
    destination_chain,
    destination_address,
    asset,
    _inserted_timestamp,
    j
FROM
    fin
