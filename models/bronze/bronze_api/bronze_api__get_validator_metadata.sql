{{ config(
  materialized = 'table'
) }}

WITH call AS (
    SELECT
        ethereum.streamline.udf_api(
            'POST',
            'https://api.axelarscan.io',{},{'path':'/cosmos/staking/v1beta1/validators','module':'lcd'}
        ) AS resp,
        SYSDATE() AS _inserted_timestamp
)
SELECT
    i.value :operator_address :: STRING AS address,
    i.value AS data,
    _inserted_timestamp
FROM call, 
LATERAL FLATTEN(
    input => resp :data :validators
) i