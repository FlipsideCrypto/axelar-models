{{ config(
    materialized = 'incremental',
    unique_key = ['tx_id','msg_index'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_id,sender,receiver);",
    tags = ['core']
) }}

WITH base_atts AS (

    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        _inserted_timestamp,
        msg_group,
        msg_sub_group,
        msg_index,
        msg_type,
        attribute_key,
        attribute_value
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type IN (
            'tx',
            'transfer'
        )

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
),
all_transfers AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        msg_group,
        msg_sub_group,
        msg_index,
        _inserted_timestamp,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :sender :: STRING AS sender,
        j :recipient :: STRING AS recipient,
        j :amount :: STRING AS amount
    FROM
        base_atts
    WHERE
        msg_type = 'transfer'
    GROUP BY
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        msg_group,
        msg_sub_group,
        msg_index,
        _inserted_timestamp
),
sender AS (
    SELECT
        tx_id,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS sender
    FROM
        base_atts
    WHERE
        msg_type = 'tx'
        AND attribute_key = 'acc_seq' qualify(ROW_NUMBER() over(PARTITION BY tx_id
    ORDER BY
        msg_index)) = 1
),
fin AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.tx_id,
        A.tx_succeeded,
        A.msg_group,
        A.msg_sub_group,
        A.msg_index,
        A._inserted_timestamp,
        COALESCE(
            A.sender,
            s.sender
        ) AS sender,
        A.recipient AS receiver,
        A.amount,
        SPLIT_PART(
            TRIM(
                REGEXP_REPLACE(
                    A.amount,
                    '[^[:digit:]]',
                    ' '
                )
            ),
            ' ',
            0
        ) AS amount_INT,
        RIGHT(A.amount, LENGTH(A.amount) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(A.amount, '[^[:digit:]]', ' ')), ' ', 0))) AS currency
    FROM
        all_transfers A
        JOIN sender s
        ON A.tx_id = s.tx_id
),
decimals AS (
    SELECT
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
        {{ ref('silver__osmo_assets') }}
        qualify(ROW_NUMBER() over(PARTITION BY denom_name
    ORDER BY
        DECIMAL DESC) = 1)
)
SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_id,
    A.tx_succeeded,
    'AXELAR' AS transfer_type,
    A.sender,
    A.amount_INT :: INT AS amount,
    A.currency,
    d.decimal,
    A.receiver,
    A.msg_index,
    b.destination_address AS foreign_address,
    b.destination_chain AS foreign_chain,
    {{ dbt_utils.generate_surrogate_key(
        ['a.tx_id','a.msg_index']
    ) }} AS transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    A._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    fin A
    LEFT JOIN decimals d
    ON A.currency = d.denom_name
    LEFT JOIN {{ ref('silver__link_events') }}
    b
    ON A.receiver = b.deposit_address
    AND len(receiver) = 65
WHERE
    A.amount IS NOT NULL
