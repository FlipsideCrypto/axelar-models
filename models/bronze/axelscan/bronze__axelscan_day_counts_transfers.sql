{{ config (
    materialized = 'view'
) }}
{{ streamline_external_table_query_v2(
    model = "axelscan_day_counts_transfers",
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER )"
) }}
