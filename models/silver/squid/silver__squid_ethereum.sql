{{ config(
    materialized = 'incremental',
    unique_key = "tx_hash",
    incremental_strategy = 'merge',
    cluster_by = 'block_timestamp::DATE',
) }}

WITH dec_logs_base AS (

    SELECT
        block_number,
        tx_hash,
        _log_id,
        event_name,
        contract_address,
        decoded_flat,
        _inserted_timestamp
    FROM
        {{ source(
            'ethereum_silver',
            'decoded_logs'
        ) }}
    WHERE
        ROUND(
            block_number,
            -3
        ) >= 14940000

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
),
logs_base AS (
    SELECT
        block_number,
        tx_hash,
        _log_id,
        block_timestamp,
        origin_from_address
    FROM
        {{ source(
            'ethereum_silver',
            'logs'
        ) }}
    WHERE
        ROUND(
            block_number,
            -3
        ) >= 14940000

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
),
squid_to_gateway AS (
    SELECT
        block_number,
        tx_hash,
        _log_id,
        contract_address AS token_address
    FROM
        dec_logs_base
    WHERE
        event_name = 'Transfer'
        AND decoded_flat :from = '0xce16f69375520ab01377ce7b88f5ba8c48f8d666'
        AND decoded_flat :to = '0x4f4495243837681061c4743b74b3eedf548d56a5'
)
SELECT
    A.block_number,
    C.block_timestamp,
    A.tx_hash,
    C.origin_from_address AS sender,
    b.token_address,
    A.decoded_flat :amount :: DECIMAL AS raw_amount,
    REGEXP_REPLACE(
        A.decoded_flat :symbol :: STRING,
        '[^a-zA-Z0-9]+'
    ) AS token_symbol,
    LOWER(
        REGEXP_REPLACE(
            A.decoded_flat :destinationChain :: STRING,
            '[^a-zA-Z0-9]+'
        )
    ) AS destination_chain,
    A.decoded_flat :sender :: STRING AS xfer_sender,
    COALESCE(
        LOWER(
            REGEXP_REPLACE(
                A.decoded_flat :destinationAddress :: STRING,
                '[^a-zA-Z0-9]+'
            )
        ),
        sender
    ) AS receiver,
    _inserted_timestamp
FROM
    dec_logs_base A
    JOIN squid_to_gateway b
    ON A.tx_hash = b.tx_hash
    AND A.block_number = b.block_number
    JOIN logs_base C
    ON A._log_id = C._log_id
    AND A.block_number = C.block_number
WHERE
    A.contract_address = '0x4f4495243837681061c4743b74b3eedf548d56a5' {# AND destinationChain <> 'osmosis' #}
