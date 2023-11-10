{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', date, address, balance_type, currency)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['date'],
) }}

WITH

{% if is_incremental() %}
recent AS (

    SELECT
        DATE,
        balance_type,
        address,
        balance,
        currency
    FROM
        {{ this }}
    WHERE
        DATE = (
            SELECT
                DATEADD('day', -1, MAX(DATE))
            FROM
                {{ this }})
        ),
        NEW AS (
            SELECT
                block_timestamp :: DATE AS DATE,
                balance_type,
                address,
                balance,
                currency,
                1 AS RANK
            FROM
                {{ ref('silver__staked_balances') }}
            WHERE
                block_timestamp :: DATE >= (
                    SELECT
                        DATEADD('day', -1, MAX(DATE))
                    FROM
                        {{ this }}) qualify(ROW_NUMBER() over (PARTITION BY block_timestamp :: DATE, address, balance_type, currency
                    ORDER BY
                        block_timestamp DESC)) = 1
                ),
                incremental AS (
                    SELECT
                        DATE,
                        balance_type,
                        address,
                        balance,
                        currency
                    FROM
                        (
                            SELECT
                                DATE,
                                balance_type,
                                address,
                                balance,
                                currency,
                                2 AS RANK
                            FROM
                                recent
                            UNION
                            SELECT
                                DATE,
                                balance_type,
                                address,
                                balance,
                                currency,
                                1 AS RANK
                            FROM
                                NEW
                        ) qualify(ROW_NUMBER() over (PARTITION BY DATE, address, balance_type, currency
                    ORDER BY
                        RANK ASC)) = 1
                ),
            {% endif %}

            base AS (

{% if is_incremental() %}
SELECT
    DATE AS block_timestamp, balance_type, address, balance, currency
FROM
    incremental
{% else %}
SELECT
    block_timestamp, balance_type, address, balance, currency
FROM
    {{ ref('silver__staked_balances') }}
{% endif %}),
address_ranges AS (
    SELECT
        address,
        balance_type,
        currency,
        MIN(
            block_timestamp :: DATE
        ) AS min_block_date,
        MAX (
            CURRENT_TIMESTAMP :: DATE
        ) AS max_block_date
    FROM
        base
    GROUP BY
        address,
        balance_type,
        currency
),
ddate AS (
    SELECT
        date_day AS DATE
    FROM
        {{ source(
            'crosschain',
            'dim_dates'
        ) }}
),
all_dates AS (
    SELECT
        d.date,
        A.balance_type,
        A.address,
        A.currency
    FROM
        ddate d
        LEFT JOIN address_ranges A
        ON d.date BETWEEN A.min_block_date
        AND A.max_block_date
    WHERE
        A.address IS NOT NULL
),
ax_balances AS (
    SELECT
        block_timestamp,
        balance_type,
        address,
        balance,
        currency
    FROM
        base qualify(ROW_NUMBER() over (PARTITION BY block_timestamp :: DATE, address, balance_type, currency
    ORDER BY
        block_timestamp DESC)) = 1
),
balance_temp AS (
    SELECT
        d.date,
        d.balance_type,
        d.address,
        b.balance,
        d.currency
    FROM
        all_dates d
        LEFT JOIN ax_balances b
        ON d.date = b.block_timestamp :: DATE
        AND d.address = b.address
        AND d.currency = b.currency
        AND d.balance_type = b.balance_type
)
SELECT
    DATE,
    balance_type,
    address,
    currency,
    LAST_VALUE(
        balance ignore nulls
    ) over(
        PARTITION BY address,
        currency,
        balance_type
        ORDER BY
            DATE ASC rows unbounded preceding
    ) AS balance,
    {{ dbt_utils.generate_surrogate_key(
        ['date','address','balance_type', 'currency']
    ) }} AS daily_balances_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    balance_temp
