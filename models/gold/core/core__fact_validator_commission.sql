{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STAKING' }} }
) }}

SELECT
    *
FROM
    {{ ref('gov__fact_validator_commission') }}
