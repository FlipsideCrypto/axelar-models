{{ config(
    materialized = 'view'
) }}

SELECT
    'arbitrum' AS blockchain,
    symbol,
    address,
    decimals
FROM
    {{ source(
        'arbitrum',
        'dim_contracts'
    ) }}
UNION ALL
SELECT
    'avalanche' AS blockchain,
    symbol,
    address,
    decimals
FROM
    {{ source(
        'avalanche',
        'dim_contracts'
    ) }}
UNION ALL
SELECT
    'ethereum' AS blockchain,
    symbol,
    address,
    decimals
FROM
    {{ source(
        'ethereum',
        'dim_contracts'
    ) }}
UNION ALL
SELECT
    'polygon' AS blockchain,
    symbol,
    address,
    decimals
FROM
    {{ source(
        'polygon',
        'dim_contracts'
    ) }}
UNION ALL
SELECT
    'bsc' AS blockchain,
    symbol,
    address,
    decimals
FROM
    {{ source(
        'bsc',
        'dim_contracts'
    ) }}
UNION ALL
SELECT
    'optimism' AS blockchain,
    symbol,
    address,
    decimals
FROM
    {{ source(
        'optimism',
        'dim_contracts'
    ) }}
UNION ALL
SELECT
    'base' AS blockchain,
    symbol,
    address,
    decimals
FROM
    {{ source(
        'base',
        'dim_contracts'
    ) }}
