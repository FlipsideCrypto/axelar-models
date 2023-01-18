{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'delete+insert',
    cluster_by = 'block_timestamp::DATE',
) }}

WITH base_atts AS (

    SELECT
        block_id,
        block_timestamp,
        blockchain,
        chain_id,
        tx_id,
        tx_succeeded,
        msg_index,
        msg_type,
        COALESCE(
            b.key,
            attribute_key
        ) AS attribute_key,
        COALESCE(
            b.value,
            attribute_value
        ) AS attribute_value,
        _inserted_timestamp
    FROM
        {{ ref('silver__msg_attributes') }},
        LATERAL FLATTEN(TRY_PARSE_JSON(attribute_value), outer => TRUE) b
    WHERE
        msg_type = 'axelar.axelarnet.v1beta1.AxelarTransferCompleted'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
msg_index AS (
    SELECT
        tx_id,
        msg_index,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        REPLACE(COALESCE(j :recipient, j :receipient), '"') :: STRING AS receiver,
        j :amount :: INT AS amount,
        j :denom :: STRING AS currency,
        REPLACE(
            j :id,
            '"'
        ) :: STRING AS transfer_id
    FROM
        base_atts v
    GROUP BY
        tx_id,
        msg_index
),
txs_final AS (
    SELECT
        block_id,
        block_timestamp,
        t.blockchain,
        chain_id,
        r.tx_id,
        tx_succeeded,
        'BRIDGE_IN' AS transfer_type,
        r.msg_index,
        NULL AS sender,
        amount,
        currency,
        receiver,
        transfer_id,
        _inserted_timestamp,
        concat_ws(
            '-',
            r.tx_id,
            r.msg_index
        ) AS _unique_key
    FROM
        msg_index r
        JOIN (
            SELECT
                DISTINCT block_id,
                block_timestamp,
                blockchain,
                chain_id,
                tx_id,
                tx_succeeded,
                _inserted_timestamp
            FROM
                base_atts
        ) t
        ON r.tx_id = t.tx_id
),
decimals AS (
    SELECT
        *,
        COALESCE (
            raw_metadata [0] :aliases [0] :: STRING,
            raw_metadata [1] :denom
        ) AS denom_name,
        CASE
            WHEN raw_metadata [0] :account_address IS NOT NULL THEN NULL
            ELSE COALESCE(
                raw_metadata [1] :exponent :: INT,
                6
            )
        END AS DECIMAL
    FROM
        {{ ref('core__dim_labels') }}
),
links AS (
    SELECT
        tx_id,
        block_id,
        source_chain,
        module,
        deposit_address,
        destination_address,
        asset
    FROM
        {{ ref('silver__link_events') }}
    WHERE
        destination_chain = 'Axelarnet'
)
SELECT
    i.block_id,
    i.block_timestamp,
    i.blockchain,
    i.chain_id,
    i.tx_id,
    i.tx_succeeded,
    i.transfer_type,
    snd.deposit_address AS sender,
    i.amount,
    i.currency,
    CASE
        WHEN i.currency IN (
            'uusd',
            'uosmo'
        ) THEN 6
        WHEN i.currency LIKE '%-wei' THEN 18
        ELSE d.decimal
    END AS DECIMAL,
    i.receiver,
    msg_index,
    transfer_id,
    _inserted_timestamp,
    _unique_key
FROM
    txs_final i
    LEFT JOIN decimals d
    ON i.currency = d.denom_name
    LEFT JOIN (
        SELECT
            destination_address,
            deposit_address
        FROM
            links z
            JOIN txs_final x
            ON x.receiver = z.destination_address
            AND z.block_id <= x.block_id qualify(ROW_NUMBER() over(PARTITION BY destination_address
        ORDER BY
            z.block_id DESC) = 1)
    ) snd
    ON i.receiver = snd.destination_address
