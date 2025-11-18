{{ config(
    materialized = 'incremental',
    unique_key = "token_symbol",
    tags = ['daily']
) }}

SELECT
    token_symbol,
    coingecko_id,
    decimals,
    api_symbol,
    token_name,
    native_chain,
    token_type,
    chains_data,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('bronze__axelscan_tokens_meta') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) - INTERVAL '1 DAY'
        FROM
            {{ this }})
        {% endif %}

        qualify ROW_NUMBER() over (
            PARTITION BY token_symbol
            ORDER BY
                CASE
                    WHEN coingecko_id IS NOT NULL THEN 1
                    ELSE 0
                END,
                _inserted_timestamp DESC
        ) = 1
