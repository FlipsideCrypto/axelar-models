{{ config(
  materialized = 'incremental',
  full_refresh = false,
  tags = ['core'],
  enabled = false
) }}

SELECT
  live.udf_api (
    'POST',
    '{Service}/{Authentication}',
    OBJECT_CONSTRUCT(
      'Content-Type',
      'application/json'
    ),
    OBJECT_CONSTRUCT(
      'id',
      1,
      'jsonrpc',
      '2.0',
      'method',
      'status'
    ),
    'Vault/prod/axelar/drpc/mainnet'
  ) AS DATA,
  SYSDATE() AS _inserted_timestamp
