{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"axelscan_searchgmp",
        "sql_limit" :"100",
        "producer_batch_size" :"20",
        "worker_batch_size" :"20",
        "sql_source" :"{{this.identifier}}",
        "order_by_column": "date_day" }
    )
) }}

WITH ids AS (

    SELECT
        _id
    FROM
        {{ source(
            'crosschain_silver',
            'number_sequence'
        ) }}
    WHERE
        _id < 200000
),
dates_hist AS (
    SELECT
        b._id -1 AS id,
        fromTime,
        toTime,
        A.date_day
    FROM
        {{ ref('streamline__axelscan_day_counts_gmp_complete') }} A
        JOIN ids b
        ON b._id <= A.day_count {# EXCEPT
    SELECT
        id,
        fromTime,
        toTime,
        date_day
    FROM
        {{ ref('streamline__axelscan_searchgmp_complete') }}
        #}
)
SELECT
    REPLACE(
        date_day :: STRING,
        '-',
        '_'
    ) AS partition_key,
    date_day,
    id,
    {{ target.database }}.live.udf_api(
        'GET',
        'https://api.gmp.axelarscan.io',
        OBJECT_CONSTRUCT(),
        OBJECT_CONSTRUCT(
            'method',
            'searchGMP',
            'fromTime',
            fromTime,
            'toTime',
            toTime,
            'from',
            id,
            'size',
            1
        )
    ) AS request
FROM
    dates_hist
ORDER BY
    date_day,
    id
