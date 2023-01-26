{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp'],
) }}

WITH all_staked AS (

    SELECT
        block_id,
        block_timestamp,
        delegator_address AS address,
        amount,
        currency,
        _inserted_timestamp
    FROM
        {{ ref('silver__staking') }}
    WHERE
        action = 'delegate'

{% if is_incremental() %}
AND block_timestamp :: DATE >=(
    SELECT
        DATEADD('day', -2, MAX(block_timestamp))
    FROM
        {{ this }})
    {% endif %}
    UNION ALL
    SELECT
        block_id,
        block_timestamp,
        delegator_address AS address,- amount,
        currency,
        _inserted_timestamp
    FROM
        {{ ref('silver__staking') }}
        s
    WHERE
        action = 'undelegate'

{% if is_incremental() %}
AND block_timestamp :: DATE >=(
    SELECT
        DATEADD('day', -2, MAX(block_timestamp))
    FROM
        {{ this }})
    {% endif %}
)
SELECT
    block_id,
    block_timestamp,
    'staked' AS balance_type,
    address,
    currency,
    SUM(amount) over(
        PARTITION BY address,
        currency
        ORDER BY
            block_timestamp ASC rows unbounded preceding
    ) AS balance,
    _inserted_timestamp
FROM
    all_staked
