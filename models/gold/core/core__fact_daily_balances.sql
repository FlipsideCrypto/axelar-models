{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'BALANCES' }} },
    tags = ['noncore']
) }}

SELECT
    DATE,
    balance_type,
    address,
    currency,
    balance,
    COALESCE(
        daily_balances_id,
        {{ dbt_utils.generate_surrogate_key(
            ['date','address','balance_type', 'currency']
        ) }}
    ) AS fact_daily_balances_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__daily_balances') }}
WHERE
    balance > 0
