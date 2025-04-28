{{ config(
    materialized = 'incremental',
    unique_key = "tx_hash",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = 'block_timestamp::DATE',
    tags = ['noncore']
) }}

WITH deco_logs_base AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_from_address,
        contract_address,
        topics,
        DATA,
        event_index,
        event_name,
        decoded_log AS decoded_flat,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ source(
            'avalanche',
            'ez_decoded_event_logs'
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
squid_to_burn AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_from_address AS eoa,
        contract_address AS token_address,
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
        A.block_timestamp,
        A.tx_hash,
        A.eoa,
        A.token_address,
        A.raw_amount,
        TRY_HEX_DECODE_STRING(SUBSTR(b.data, 3 + (64 * 6), 16)) AS destination_chain,
        TRY_HEX_DECODE_STRING(RIGHT(b.data, 64)) AS token_symbol,
        A._inserted_timestamp
    FROM
        squid_to_burn A
        LEFT JOIN (
            SELECT
                b.data,
                b.tx_hash,
                b.block_number
            FROM
                deco_logs_base b
            WHERE
                b.topics [0] = '0x7e50569d26be643bda7757722291ec66b1be66d8283474ae3fab5a98f878a7a2'
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
        deco_logs_base A
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
        CASE
            WHEN LOWER(REGEXP_REPLACE(destination_chain, '[^a-zA-Z0-9]+')) = 'avalanch' THEN 'avalanche'
            ELSE LOWER(REGEXP_REPLACE(destination_chain, '[^a-zA-Z0-9]+'))
        END AS destination_chain,
        LOWER(receiver) AS receiver,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash']
        ) }} AS squid_avalanche_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        _inserted_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        arb_result A
