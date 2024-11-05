{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"axelscan_day_counts_transfers",
        "sql_limit" :"100",
        "producer_batch_size" :"20",
        "worker_batch_size" :"20",
        "sql_source" :"{{this.identifier}}",
        "order_by_column": "date_day" }
    )
) }}

WITH dates_hist AS (

    SELECT
        A.date_day,
        DATE_PART(
            epoch_second,
            A.date_day
        ) AS fromTime,
        DATE_PART(epoch_second, DATEADD (DAY, 1, A.date_day)) -1 AS toTime
    FROM
        {{ source(
            'crosschain',
            'dim_dates'
        ) }} A

{% if is_incremental() %}
LEFT JOIN {{ ref('streamline__axelscan_day_counts_transfers_complete') }}
b
ON A.date_day = b.date_day
WHERE
    b.date_day IS NULL
    AND A.date_day BETWEEN '2021-12-23'
    AND SYSDATE() :: DATE - 2
{% else %}
WHERE
    A.date_day BETWEEN '2021-12-23'
    AND SYSDATE() :: DATE - 2
{% endif %}
),
dates_recent AS (
    SELECT
        date_day,
        DATE_PART(
            epoch_second,
            date_day
        ) AS fromTime,
        DATE_PART(epoch_second, DATEADD (DAY, 1, date_day)) -1 AS toTime
    FROM
        {{ source(
            'crosschain',
            'dim_dates'
        ) }}
    WHERE
        date_day BETWEEN SYSDATE() :: DATE - 1
        AND SYSDATE() :: DATE
),
date_combo AS (
    SELECT
        date_day,
        fromTime,
        toTime
    FROM
        dates_hist
    UNION ALL
    SELECT
        date_day,
        fromTime,
        toTime
    FROM
        dates_recent
)
SELECT
    REPLACE(
        date_day :: STRING,
        '-',
        '_'
    ) AS partition_key,
    date_day,
    fromTime,
    toTime,
    {{ target.database }}.live.udf_api(
        'GET',
        'https://api.axelarscan.io',
        OBJECT_CONSTRUCT(),
        OBJECT_CONSTRUCT(
            'method',
            'searchTransfers',
            'fromTime',
            fromTime,
            'toTime',
            toTime,
            'size',
            1
        )
    ) AS request
FROM
    date_combo
ORDER BY
    date_day
