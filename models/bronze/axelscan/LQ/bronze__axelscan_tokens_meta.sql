{{ config(
    materialized = 'incremental',
    tags = ['daily']
) }}

WITH tokens AS (

    SELECT
        token_symbol,
        COUNT(1) AS xc
    FROM
        axelar.axelscan.ez_bridge_activity -- hard code to avoid ref cycle
    WHERE
        token_symbol IS NOT NULL
    GROUP BY
        token_symbol
),
WORK AS (
    SELECT
        A.token_symbol
    FROM
        tokens A

{% if is_incremental() %}
LEFT JOIN {{ this }}
b
ON A.token_symbol = b.token_symbol
WHERE
    b.token_symbol IS NULL
    OR b._inserted_timestamp <= DATEADD(DAY, -3, SYSDATE())
{% endif %}
ORDER BY
    xc DESC
LIMIT
    50), api_calls AS (
        SELECT
            token_symbol,
            live.udf_api(
                'POST',
                'https://api.axelarscan.io/api/getTokensPrice',
                OBJECT_CONSTRUCT(
                    'accept',
                    'application/json',
                    'content-type',
                    'application/json'
                ),
                OBJECT_CONSTRUCT(
                    'symbol',
                    token_symbol
                )
            ) AS response,
            SYSDATE() AS _inserted_timestamp
        FROM
            WORK
    )
SELECT
    token_symbol,
    response,
    response :status_code :: INT AS status_code,
    response :data AS price_data,
    -- Parse the price data for the token
    -- API returns data with token symbol as key
    response :data [token_symbol] :price :: FLOAT AS price,
    response :data [token_symbol] :coingecko_id :: STRING AS coingecko_id,
    response :data [token_symbol] :decimals :: INT AS decimals,
    response :data [token_symbol] :symbol :: STRING AS api_symbol,
    response :data [token_symbol] :name :: STRING AS token_name,
    response :data [token_symbol] :native_chain :: STRING AS native_chain,
    response :data [token_symbol] :type :: STRING AS token_type,
    response :data [token_symbol] :chains AS chains_data,
    -- Time tracking
    _inserted_timestamp
FROM
    api_calls
