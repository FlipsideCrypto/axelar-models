{{ config(
    materialized = 'incremental',
    unique_key = ['day_utc'],
    incremental_strategy = 'delete+insert',
    tags = ['daily']
) }}

WITH gmp_stats AS (
    SELECT
        date_day,
        source_chain,
        destination_chain,
        num_txs AS path_txs,
        volume_usd AS path_volume
    FROM
        {{ ref('silver__axelscan_gmp_stats_by_chains') }}

{% if is_incremental() %}
    WHERE
        date_day >= (
            SELECT
                COALESCE(MAX(day_utc), '1970-01-01' :: DATE) - 7
            FROM
                {{ this }}
        )
{% endif %}
),
transfer_stats AS (
    SELECT
        date_day,
        source_chain,
        destination_chain,
        num_txs AS path_txs,
        volume_usd AS path_volume
    FROM
        {{ ref('silver__axelscan_transfer_stats_by_chains') }}

{% if is_incremental() %}
    WHERE
        date_day >= (
            SELECT
                COALESCE(MAX(day_utc), '1970-01-01' :: DATE) - 7
            FROM
                {{ this }}
        )
{% endif %}
),
    combined AS (
        SELECT
            COALESCE(
                g.source_chain,
                t.source_chain
            ) AS source_blockchain,
            COALESCE(
                g.destination_chain,
                t.destination_chain
            ) AS destination_blockchain,
            COALESCE(
                g.date_day,
                t.date_day
            ) AS day_utc,
            COALESCE(
                g.path_txs,
                0
            ) AS gmp_num_txs,
            COALESCE(
                g.path_volume,
                0
            ) AS gmp_volume_usd,
            COALESCE(
                t.path_txs,
                0
            ) AS transfers_num_txs,
            COALESCE(
                t.path_volume,
                0
            ) AS transfers_volume_usd
        FROM
            gmp_stats g full
            OUTER JOIN transfer_stats t
            ON g.date_day = t.date_day
            AND g.source_chain = t.source_chain
            AND g.destination_chain = t.destination_chain
    )
SELECT
    REPLACE(source_blockchain, 'axelarnet', 'axelar') AS source_blockchain,
    REPLACE(destination_blockchain, 'axelarnet', 'axelar') AS destination_blockchain,
    day_utc,
    COALESCE(
        gmp_num_txs + transfers_num_txs,
        0
    ) AS num_txs,
    COALESCE(
        gmp_volume_usd + transfers_volume_usd,
        0
    ) AS volume_usd,
    COALESCE(
        gmp_num_txs,
        0
    ) AS gmp_num_txs,
    COALESCE(
        gmp_volume_usd,
        0
    ) AS gmp_volume_usd,
    COALESCE(
        transfers_num_txs,
        0
    ) AS transfers_num_txs,
    COALESCE(
        transfers_volume_usd,
        0
    ) AS transfers_volume_usd,
    {{ dbt_utils.generate_surrogate_key(
        ['source_blockchain', 'destination_blockchain', 'day_utc']
    ) }} AS ez_bridge_metrics_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    combined qualify ROW_NUMBER() over (
        PARTITION BY source_blockchain,
        destination_blockchain,
        day_utc
        ORDER BY
            inserted_timestamp DESC
    ) = 1
