{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['noncore']
) }}

WITH txs AS (

    SELECT
        DISTINCT A.tx_id,
        A.msg_group
    FROM
        {{ ref('silver__msg_attributes') }} A
    WHERE
        msg_type = 'withdraw_commission'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
),
msg_attributes_base AS (
    SELECT
        A.tx_id,
        A.block_id,
        A.block_timestamp,
        A.tx_succeeded,
        A.msg_type,
        A.msg_group,
        A.msg_index,
        A.attribute_key,
        A.attribute_value,
        A._inserted_timestamp
    FROM
        {{ ref('silver__msg_attributes') }} A
        JOIN txs b
        ON A.tx_id = b.tx_id
    WHERE
        (
            A.msg_group = b.msg_group
            OR (
                A.msg_group IS NULL
                AND msg_type || attribute_key = 'txacc_seq'
            )
        )
        AND msg_type || attribute_key IN (
            'withdraw_commissionamount',
            'transferrecipient',
            'messagesender',
            'txacc_seq'
        )
        AND NOT (
            msg_type || attribute_key = 'messagesender'
            AND len(attribute_value) = 45
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
),
combo AS (
    SELECT
        tx_id,
        MAX(msg_group) AS msg_group,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        SPLIT_PART(
            j :acc_seq :: STRING,
            '/',
            0
        ) AS tx_caller_address,
        j :recipient :: STRING AS validator_address_reward,
        j :sender :: STRING AS validator_address_operator,
        j :amount :: STRING AS amount
    FROM
        msg_attributes_base
    GROUP BY
        tx_id
),
block_tx_inserted AS (
    SELECT
        DISTINCT A.tx_id,
        A.block_id,
        A.block_timestamp,
        A.tx_succeeded,
        A._inserted_timestamp
    FROM
        msg_attributes_base A
)
SELECT
    b.block_id,
    b.block_timestamp,
    A.tx_id,
    b.tx_succeeded,
    A.tx_caller_address,
    A.msg_group,
    REPLACE(
        A.amount,
        'uaxl'
    ) :: INT AS amount,
    'uaxl' AS currency,
    A.validator_address_operator,
    A.validator_address_reward,
    {{ dbt_utils.generate_surrogate_key(
        ['a.tx_id']
    ) }} AS staking_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    b._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    combo A
    JOIN block_tx_inserted b
    ON A.tx_id = b.tx_id
