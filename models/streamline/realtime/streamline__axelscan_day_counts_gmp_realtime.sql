{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_rest_api',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"axelscan_day_counts_gmp",
        "sql_limit" :"12000",
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
        from_time + 59 AS TO_TIME
    FROM
        {{ source(
            'crosschain',
            'dim_dates'
        ) }} A
        JOIN TABLE(GENERATOR(rowcount => 1440)) x
    WHERE
        A.date_day BETWEEN '2024-12-10'
        AND SYSDATE() :: DATE - 1
),
exclude_done AS (
    SELECT
        A.date_day,
        A.from_time,
        A.to_time
    FROM
        dates_hist A
        LEFT JOIN {{ ref('streamline__axelscan_day_counts_gmp_complete') }}
        b
        ON A.date_day = b.date_day
        AND A.from_time = b.from_time
    WHERE
        b.date_day IS NULL
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
    exclude_done
