{{ config(
    materialized = 'incremental',
    unique_key = "id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = 'created_at::DATE',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(id);",
    tags = ['noncore'],
    enabled = false
) }}

WITH base AS (

    SELECT
        id,
        DATA,
        _inserted_timestamp
    FROM
        {{ ref('bronze_api__axelscan_searchgmp') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp :: DATE >= (
        SELECT
            MAX(_inserted_timestamp) :: DATE
        FROM
            {{ this }}
    )
{% endif %}

qualify (ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC) = 1)
)
SELECT
    id,
    TO_TIMESTAMP(
        COALESCE(
            DATA :call :created_at :ms,
            DATA :gas_paid :created_at :ms,
            DATA :approved :created_at :ms,
            DATA :express_executed :created_at :ms,
            DATA :confirm :block_timestamp
        ) :: STRING
    ) :: datetime AS created_at,
    DATA :status :: STRING AS status,
    DATA :amount :: FLOAT AS amount,
    COALESCE(
        DATA :call :destination_chain_type,
        DATA :gas_paid :destination_chain_type
    ) :: STRING AS destination_chain_type,
    COALESCE(
        DATA :call :event,
        DATA :gas_paid :event,
        DATA :approved :event,
        DATA :express_executed :event,
        DATA :confirm :event
    ) :: STRING AS event,
    DATA :simplified_status :: STRING AS simplified_status,
    DATA :call AS call,
    DATA :command_id :: STRING AS command_id,
    DATA :fees AS fees,
    DATA :gas_status :: STRING AS gas_status,
    DATA :is_call_from_relayer :: STRING AS is_call_from_relayer,
    DATA :is_invalid_call :: STRING AS is_invalid_call,
    DATA :is_invalid_destination_chain :: STRING AS is_invalid_destination_chain,
    DATA :is_two_way :: STRING AS is_two_way,
    DATA :time_spent AS time_spent,
    DATA,
    {{ dbt_utils.generate_surrogate_key(
        ['id']
    ) }} AS axelscan_gmp_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base
