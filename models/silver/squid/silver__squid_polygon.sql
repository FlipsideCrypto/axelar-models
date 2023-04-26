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
        event_index,
        origin_from_address,
        contract_address,
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
deco_logs_base AS (
    SELECT
        block_number,
        tx_hash,
        event_index,
        event_name,
        decoded_flat,
        _inserted_timestamp
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

{% if is_incremental() %}
AND block_number >= (
    SELECT
        MAX(block_number)
    FROM
        {{ this }}
)
{% endif %}
),
squid_to_burn AS (
    SELECT
        block_number,
        tx_hash,
        event_index,
        decoded_flat :value AS raw_amount,
        _inserted_timestamp
    FROM
        deco_logs_base
    WHERE
        event_name = 'Transfer'
        AND decoded_flat :from = '0xce16f69375520ab01377ce7b88f5ba8c48f8d666'
        AND decoded_flat :to = '0x0000000000000000000000000000000000000000'
),
all_transfers AS (
    SELECT
        A.block_number,
        lb.block_timestamp,
        A.tx_hash,
        lb.origin_from_address AS eoa,
        lb.contract_address AS token_address,
        A.raw_amount,
        TRY_HEX_DECODE_STRING(SUBSTR(b.data, 3 + (64 * 7), 16)) AS destination_chain,
        TRY_HEX_DECODE_STRING(RIGHT(b.data, 64)) AS token_symbol,
        A._inserted_timestamp
    FROM
        squid_to_burn A
        JOIN logs_base lb
        ON A.tx_hash = lb.tx_hash
        AND A.block_number = lb.block_number
        AND A.event_index = lb.event_index
        LEFT JOIN (
            SELECT
                b.data,
                b.tx_hash,
                b.block_number
            FROM
                logs_base b
            WHERE
                b.topics [0] = '0x999d431b58761213cf53af96262b67a069cbd963499fd8effd1e21556217b841'
        ) b
        ON A.tx_hash = b.tx_hash
        AND A.block_number = b.block_number
    WHERE
        raw_amount IS NOT NULL
),
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
                TRY_HEX_DECODE_STRING(
                    DATA [7]
                ) || TRY_HEX_DECODE_STRING(
                    DATA [8]
                ) AS receiver,
                _inserted_timestamp
            FROM
                nonevm_transfers A
                JOIN nonevm_fix_data b
                ON A.tx_hash = b.tx_hash
        ),
        arb_result AS (
            SELECT
                block_number,
                block_timestamp,
                tx_hash,
                eoa,
                token_address,
                raw_amount :: DECIMAL AS raw_amount,
                token_symbol,
                destination_chain,
                eoa AS receiver,
                _inserted_timestamp
            FROM
                evm_transfers
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
                receiver,
                _inserted_timestamp
            FROM
                non_evm_fix
        )
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
        LOWER(receiver) AS receiver,
        _inserted_timestamp
    FROM
        arb_result A
