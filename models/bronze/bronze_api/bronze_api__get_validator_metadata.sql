{{ config(
    materialized = 'table'
) }}

WITH call AS (

    SELECT
        live.udf_api(
            'GET',
            'https://api.axelarscan.io/validator/getValidators',{},{}
        ) AS resp,
        SYSDATE() AS _inserted_timestamp
) {# ,
call_2 AS (
    SELECT
        live.udf_api(
            'POST',
            'https://api.axelarscan.io',{},{ 'path' :'/cosmos/staking/v1beta1/validators?pagination.key=FMrVj6JowYsKRr2zLFCWofPkvPYV',
            'module' :'lcd' }
        ) AS resp,
        SYSDATE() AS _inserted_timestamp
) #}
SELECT
    i.value :operator_address :: STRING AS address,
    i.value AS DATA,
    _inserted_timestamp
FROM
    call,
    LATERAL FLATTEN(
        input => resp :data :data
    ) i {# UNION ALL
SELECT
    i.value :operator_address :: STRING AS address,
    i.value AS DATA,
    _inserted_timestamp
FROM
    call,
    LATERAL FLATTEN(
        input => resp :data :validators
    ) i #}
