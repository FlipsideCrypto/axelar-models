{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_rest_api',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"axelscan_day_counts_gmp",
        "sql_limit" :"4000",
        "producer_batch_size" :"200",
        "worker_batch_size" :"200",
        "sql_source" :"{{this.identifier}}",
        "order_by_column": "ob",
        "async_concurrent_requests": "5" }
    ),
    tags = ['streamline_axelscan']
) }}

WITH dates_hist AS (

    SELECT
        A.date_day,
        (ROW_NUMBER() over (PARTITION BY A.date_day
    ORDER BY
        SEQ4()) - 1) * 60 + DATE_PART(
            epoch_second,
            A.date_day
        ) AS from_time,
        from_time AS ft,
        from_time + 59 AS TO_TIME
    FROM
        {{ source(
            'crosschain',
            'dim_dates'
        ) }} A
        JOIN TABLE(GENERATOR(rowcount => 1440)) x
        LEFT JOIN {{ ref('streamline__axelscan_day_counts_gmp_complete') }}
        b
        ON A.date_day = b.date_day
        AND ft = b.from_time
    WHERE
        A.date_day BETWEEN '2024-12-10'
        AND SYSDATE() :: DATE - 2
        AND b.date_day IS NULL
),
dates_recent AS (
    SELECT
        date_day,
        (ROW_NUMBER() over (PARTITION BY date_day
    ORDER BY
        SEQ4()) - 1) * 60 + DATE_PART(
            epoch_second,
            date_day
        ) AS from_time,
        from_time + 59 AS TO_TIME
    FROM
        {{ source(
            'crosschain',
            'dim_dates'
        ) }}
        JOIN TABLE(GENERATOR(rowcount => 1440)) x
    WHERE
        date_day BETWEEN SYSDATE() :: DATE - 1
        AND SYSDATE() :: DATE
),
date_combo AS (
    SELECT
        date_day,
        ft AS from_time,
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
    partition_key || '-' || from_time :: STRING AS ob,
    {{ target.database }}.live.udf_api(
        'POST',
        'https://api.gmp.axelarscan.io',
        OBJECT_CONSTRUCT(),
        OBJECT_CONSTRUCT(
            'method',
            'searchGMP',
            'fromTime',
            from_time,
            'toTime',
            TO_TIME,
            'size',
            1
        )
    ) AS request
FROM
    date_combo
