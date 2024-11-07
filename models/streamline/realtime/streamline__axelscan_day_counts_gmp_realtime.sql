{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_rest_api',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"axelscan_day_counts_gmp",
        "sql_limit" :"200",
        "producer_batch_size" :"100",
        "worker_batch_size" :"100",
        "sql_source" :"{{this.identifier}}",
        "order_by_column": "date_day" }
    ),
    tags = ['streamline_axelscan']
) }}

WITH dates_hist AS (

    SELECT
        A.date_day,
        DATE_PART(
            epoch_second,
            A.date_day
        ) AS from_time,
        DATE_PART(epoch_second, DATEADD (DAY, 1, A.date_day)) -1 AS TO_TIME
    FROM
        {{ source(
            'crosschain',
            'dim_dates'
        ) }} A
        LEFT JOIN {{ ref('streamline__axelscan_day_counts_gmp_complete') }}
        b
        ON A.date_day = b.date_day
    WHERE
        A.date_day BETWEEN '2022-05-09'
        AND SYSDATE() :: DATE - 2
        AND b.date_day IS NULL
),
dates_recent AS (
    SELECT
        date_day,
        DATE_PART(
            epoch_second,
            date_day
        ) AS from_time,
        DATE_PART(epoch_second, DATEADD (DAY, 1, date_day)) -1 AS TO_TIME
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
        from_time,
        TO_TIME
    FROM
        dates_hist
    UNION ALL
    SELECT
        date_day,
        from_time,
        TO_TIME
    FROM
        dates_recent
)
SELECT
    REPLACE(
        date_day :: STRING,
        '-'
    ) AS partition_key,
    from_time,
    TO_TIME,
    {{ target.database }}.live.udf_api(
        'GET',
        'https://api.gmp.axelarscan.io',
        OBJECT_CONSTRUCT(),
        OBJECT_CONSTRUCT(
            'method',
            'searchGMP',
            'from_time',
            from_time,
            'to_time',
            TO_TIME,
            'size',
            1
        )
    ) AS request
FROM
    date_combo
