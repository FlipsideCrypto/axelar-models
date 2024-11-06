{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_rest_api',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"axelscan_searchtransfers",
        "sql_limit" :"10000",
        "producer_batch_size" :"5000",
        "worker_batch_size" :"5000",
        "sql_source" :"{{this.identifier}}",
        "order_by_column": "ob" }
    ),
    tags = ['streamline_axelscan']
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
        {{ ref('streamline__axelscan_day_counts_transfers_complete') }} A
        JOIN ids b
        ON b._id <= A.day_count
),
ids_topull AS (
    SELECT
        A.id,
        A.fromTime,
        A.toTime,
        A.date_day
    FROM
        dates_hist A
        LEFT JOIN {{ ref('streamline__axelscan_searchtransfers_complete') }}
        b
        ON A.date_day = b.date_day
        AND A.id = b.id
    WHERE
        b.date_day IS NULL
)
SELECT
    REPLACE(
        date_day :: STRING,
        '-'
    ) AS partition_key,
    id,
    partition_key || id :: STRING AS ob,
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
            'from',
            id,
            'size',
            1
        )
    ) AS request
FROM
    ids_topull
