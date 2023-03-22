{{ config(
    materialized = 'incremental',
    unique_key = "tx_hash",
    incremental_strategy = 'merge',
    cluster_by = 'block_timestamp::DATE',
) }}

WITH logs_base AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_name,
        origin_from_address,
        contract_address,
        event_inputs,
        topics,
        DATA,
        _inserted_timestamp
    FROM
        {{ source(
            'polygon_silver',
            'logs'
        ) }}
    WHERE
        block_timestamp :: DATE >= '2022-11-01'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
),
decode_base AS (
    SELECT
        DISTINCT block_number,
        tx_hash,
        decoded_flat :amount AS amount,
        decoded_flat :destinationChain AS destinationChain,
        decoded_flat :symbol AS symbol
    FROM
        {{ source(
            'polygon_silver',
            'decoded_logs'
        ) }}
    WHERE
        ROUND(
            block_number,
            -3
        ) >= 35046000
        AND (
            (
                event_name = 'ContractCallWithToken'
                AND contract_address = '0x6f015f16de9fc8791b234ef68d486d2bf203fba8'
            )
            OR (
                event_name = 'NativeGasPaidForContractCallWithToken'
                AND contract_address = '0x2d5d7d31f671f86c782533cc367f14109a082712'
            )
        )

{% if is_incremental() %}
AND ROUND(
    block_number,
    -3
) >= (
    SELECT
        MAX(ROUND(block_number, -3))
    FROM
        {{ this }}
)
{% endif %}
),
squid_to_burn AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_from_address AS eoa,
        contract_address AS token_address,
        event_inputs :value AS raw_amount,
        _inserted_timestamp
    FROM
        logs_base
    WHERE
        event_name = 'Transfer'
        AND event_inputs :from = '0xce16f69375520ab01377ce7b88f5ba8c48f8d666'
        AND event_inputs :to = '0x0000000000000000000000000000000000000000'
),
all_transfers AS (
    SELECT
        A.block_number,
        A.block_timestamp,
        A.tx_hash,
        A.eoa,
        A.token_address,
        A.raw_amount,
        b.destinationchain :: STRING AS destination_chain,
        b.symbol :: STRING AS token_symbol,
        _inserted_timestamp
    FROM
        squid_to_burn A
        LEFT JOIN decode_base b
        ON A.tx_hash = b.tx_hash
        AND A.block_number = b.block_number
    WHERE
        raw_amount IS NOT NULL
) {# ,
evm_transfers AS (
    SELECT
        A.block_number,
        A.block_timestamp,
        A.tx_hash,
        A.eoa,
        A.token_address,
        A.raw_amount,
        A.destination_chain,
        A.token_symbol,
        _inserted_timestamp
    FROM
        all_transfers A
    WHERE
        destination_chain IS NOT NULL
),
nonevm_transfers AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        eoa,
        token_address,
        raw_amount,
        _inserted_timestamp
    FROM
        all_transfers
    WHERE
        destination_chain IS NULL
),
nonevm_fix_data AS (
    SELECT
        A.block_number,
        A.block_timestamp,
        A.tx_hash,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS DATA
    FROM
        logs_base A
        JOIN nonevm_transfers b
        ON A.tx_hash = b.tx_hash
        AND A.block_number = b.block_number
    WHERE
        A.topics [0] = '0x651d93f66c4329630e8d0f62488eff599e3be484da587335e8dc0fcf46062726'),
        non_evm_fix AS (
            SELECT
                A.block_number,
                A.block_timestamp,
                A.tx_hash,
                A.eoa,
                token_address,
                A.raw_amount :: DECIMAL AS raw_amount,
                TRY_HEX_DECODE_STRING(
                    DATA [10]
                ) AS token_symbol,
                TRY_HEX_DECODE_STRING(
                    DATA [5]
                ) AS destination_chain,
                _inserted_timestamp
            FROM
                nonevm_transfers A
                JOIN nonevm_fix_data b
                ON A.tx_hash = b.tx_hash
        ),
        #}
        {# arb_result AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        eoa,
        token_address,
        raw_amount :: DECIMAL AS raw_amount,
        token_symbol,
        destination_chain,
        _inserted_timestamp
    FROM
        evm_traansfers
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        eoa,
        token_address,
        raw_amount,
        token_symbol,
        destination_chain,
        _inserted_timestamp
    FROM
        non_evm_fix
) #}
SELECT
    A.block_number,
    block_timestamp,
    A.tx_hash,
    eoa AS sender,
    token_address,
    raw_amount :: DECIMAL AS raw_amount,
    REGEXP_REPLACE(
        token_symbol,
        '[^a-zA-Z0-9]+'
    ) AS token_symbol,
    LOWER(REGEXP_REPLACE(destination_chain, '[^a-zA-Z0-9]+')) AS destination_chain,
    _inserted_timestamp
FROM
    all_transfers A
