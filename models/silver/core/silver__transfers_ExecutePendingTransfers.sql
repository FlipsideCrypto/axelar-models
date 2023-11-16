{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'delete+insert',
    cluster_by = 'block_timestamp::DATE',
    tags = ['core']
) }}

WITH base_atts AS (

    SELECT
        block_id,
        block_timestamp,
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
        msg_type IN (
            'axelar.axelarnet.v1beta1.AxelarTransferCompleted',
            'transfer'
        )

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
    WHERE
        msg_type = 'axelar.axelarnet.v1beta1.AxelarTransferCompleted'
    GROUP BY
        tx_id,
        msg_index
),
txn AS (
    SELECT
        tx_id,
        msg_index,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :sender :: STRING AS sender,
        j :recipient :: STRING AS receiver,
        COALESCE(
            SPLIT_PART(
                TRIM(
                    REGEXP_REPLACE(
                        j :amount,
                        '[^[:digit:]]',
                        ' '
                    )
                ),
                ' ',
                0
            ),
            TRY_PARSE_JSON(
                j :amount
            ) :amount
        ) AS amount
    FROM
        base_atts
    WHERE
        msg_type = 'transfer'
    GROUP BY
        tx_id,
        msg_index
),
txs_final AS (
    SELECT
        block_id,
        block_timestamp,
        r.tx_id,
        tx_succeeded,
        'IBC_TRANSFER_IN' AS transfer_type,
        r.msg_index,
        snd.sender AS sender,
        r.amount,
        currency,
        r.receiver,
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
                tx_id,
                tx_succeeded,
                _inserted_timestamp
            FROM
                base_atts
        ) t
        ON r.tx_id = t.tx_id
        LEFT JOIN txn snd
        ON r.tx_id = snd.tx_id
        AND snd.msg_index < r.msg_index
        AND r.receiver = snd.receiver
        AND r.amount = snd.amount
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
    i.tx_id,
    i.tx_succeeded,
    i.transfer_type,
    i.sender,
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
    snd.deposit_address AS foreign_address,
    snd.source_chain AS foreign_chain,
    _unique_key,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id','msg_index']
    ) }} AS transfer_executependingtransfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    txs_final i
    LEFT JOIN decimals d
    ON i.currency = d.denom_name
    LEFT JOIN (
        SELECT
            destination_address,
            deposit_address,
            source_chain
        FROM
            links z
            JOIN txs_final x
            ON x.receiver = z.destination_address
            AND z.block_id <= x.block_id qualify(ROW_NUMBER() over(PARTITION BY destination_address
        ORDER BY
            z.block_id DESC) = 1)
    ) snd
    ON i.receiver = snd.destination_address
