{{ config(
    materialized = 'incremental',
    unique_key = "tx_hash",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = 'block_timestamp::DATE',
    tags = ['noncore']
) }}

WITH dec_logs_base AS (

    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        tx_hash,
        event_index,
        event_name,
        contract_address,
        decoded_log AS decoded_flat,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ source(
            'ethereum',
            'ez_decoded_event_logs'
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
        event_index,
        contract_address AS token_address
    FROM
        dec_logs_base
    WHERE
        event_name = 'Transfer'
        AND decoded_flat :from = '0xce16f69375520ab01377ce7b88f5ba8c48f8d666'
        AND decoded_flat :to = '0x4f4495243837681061c4743b74b3eedf548d56a5' qualify(ROW_NUMBER() over(PARTITION BY tx_hash
    ORDER BY
        event_index DESC) = 1)
)
SELECT
    A.block_number,
    A.block_timestamp,
    A.tx_hash,
    A.origin_from_address AS sender,
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
        LOWER(
            REGEXP_REPLACE(
                A.decoded_flat :destinationContractAddress :: STRING,
                '[^a-zA-Z0-9]+'
            )
        ),
        sender
    ) AS receiver,
    {{ dbt_utils.generate_surrogate_key(
        ['a.tx_hash']
    ) }} AS squid_ethereum_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    dec_logs_base A
    JOIN squid_to_gateway b
    ON A.tx_hash = b.tx_hash
    AND A.block_number = b.block_number
WHERE
    A.contract_address = '0x4f4495243837681061c4743b74b3eedf548d56a5' {# AND destinationChain <> 'osmosis' #}
    qualify(ROW_NUMBER() over(PARTITION BY A.tx_hash
ORDER BY
    A.event_index DESC) = 1)
