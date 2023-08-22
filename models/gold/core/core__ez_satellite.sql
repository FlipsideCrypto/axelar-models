{{ config(
    materialized = 'view'
) }}

SELECT
    *
FROM
    {{ ref('defi__ez_bridge_satellite') }}
