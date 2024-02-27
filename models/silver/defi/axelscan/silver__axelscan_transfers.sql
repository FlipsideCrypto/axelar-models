{{ config(
    materialized = 'incremental',
    unique_key = "id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = 'created_at::DATE',
    tags = ['noncore']
) }}

WITH base AS (

    SELECT
        id,
        DATA,
        _inserted_timestamp
    FROM
        {{ ref('bronze_api__axelscan_searchtransfers') }}

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
        DATA :send :created_at :ms :: STRING
    ) :: datetime AS created_at,
    DATA :status :: STRING AS status,
    DATA :send :amount :: FLOAT AS send_amount,
    DATA :send :amount_received :: FLOAT AS send_amount_received,
    DATA :send :fee :: FLOAT AS send_fee,
    DATA :send :denom :: STRING AS send_denom,
    DATA :send :source_chain :: STRING AS source_chain,
    DATA :send :destination_chain :: STRING AS destination_chain,
    DATA :send :sender_address :: STRING AS sender_address,
    DATA :send :recipient_address :: STRING AS recipient_address,
    DATA :simplified_status :: STRING AS simplified_status,
    DATA :link AS link,
    DATA :send AS send,
    DATA,
    {{ dbt_utils.generate_surrogate_key(
        ['id']
    ) }} AS axelscan_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base
