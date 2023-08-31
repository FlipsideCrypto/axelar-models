{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'SQUID',
    'PURPOSE': 'DEFI' }} }
) }}

SELECT
    *
FROM
    {{ ref('defi__ez_bridge_squid') }}
