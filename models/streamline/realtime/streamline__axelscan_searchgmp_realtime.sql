{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_rest_api',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"axelscan_searchgmp",
        "sql_limit" :"4000",
        "producer_batch_size" :"100",
        "worker_batch_size" :"100",
        "sql_source" :"{{this.identifier}}",
        "order_by_column": "ob",
        "async_concurrent_requests" :"5" }
    ),
    tags = ['streamline_axelscan']
) }}
--This is the max number of ids that the api supports for the searchGMP method
{% set limit = var(
    'AXELSCAN_ID_LIMIT', 501
) %}
WITH ids AS (

    SELECT
        _id
    FROM
        {{ source(
            'crosschain_silver',
            'number_sequence'
        ) }}
    WHERE
        _id < {{ limit }}
), dates_hist AS (
    SELECT
        b._id -1 AS id,
        from_time,
        TO_TIME,
        A.date_day
    FROM
        {{ ref('streamline__axelscan_day_counts_gmp_complete') }} A
        JOIN ids b
        ON b._id <= A.day_count
),
ids_topull AS (
    SELECT
        A.id,
        A.from_time,
        A.to_time,
        A.date_day
    FROM
        dates_hist A
        LEFT JOIN {{ ref('streamline__axelscan_searchgmp_complete') }}
        b
        ON A.date_day = b.date_day
        AND A.id = b.id
        AND A.from_time = b.from_time
    WHERE
        b.date_day IS NULL
)
SELECT
    REPLACE(
        date_day :: STRING,
        '-'
    ) AS partition_key,
    id,
    from_time,
    TO_TIME,
    partition_key || '-' || from_time || '-' || id :: STRING AS ob,
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
            'from',
            id,
            'size',
            1
        )
    ) AS request
FROM
    ids_topull
ORDER BY
    ob
