{{ config(
    materialized = 'incremental',
    unique_key = ['date_day', 'source_chain', 'destination_chain'],
    incremental_strategy = 'delete+insert',
    tags = ['daily']
) }}

WITH parsed_gmp_stats AS (
    SELECT
        date_day,
        b.value :key :: STRING AS source_chain,
        C.value :key :: STRING AS destination_chain,
        C.value :num_txs :: INT AS num_txs,
        C.value :volume :: DECIMAL(18, 2) AS volume_usd,
        _inserted_timestamp
    FROM
        {{ ref('bronze__axelscan_gmp_stats_by_chains') }},
        LATERAL FLATTEN(resp :data :source_chains) b,
        LATERAL FLATTEN(b.value :destination_chains) C

{% if is_incremental() %}
    WHERE
        date_day >= (
            SELECT
                COALESCE(MAX(date_day), '1970-01-01' :: DATE) - 3
            FROM
                {{ this }}
        )
{% endif %}
)
SELECT
    date_day,
    source_chain,
    destination_chain,
    num_txs,
    volume_usd,
    _inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    parsed_gmp_stats
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY date_day, source_chain, destination_chain
        ORDER BY _inserted_timestamp DESC
    ) = 1
