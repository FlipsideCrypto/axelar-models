{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

SELECT
    {{ target.database }}.live.udf_api(
        'POST',
        '{Service}/{Authentication}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json',
            'fsc-quantum-state',
            'livequery'
        ),
        OBJECT_CONSTRUCT(
            'id',
            0,
            'jsonrpc',
            '2.0',
            'method',
            'status',
            'params',
            []
        ),
        'vault/prod/axelar/drpc/mainnet'
    ) :data :result :sync_info :latest_block_height :: INT AS block_number
