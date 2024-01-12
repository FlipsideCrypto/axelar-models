{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['noncore']
) }}

WITH msg_attributes_base AS (

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
    WHERE
        msg_type IN (
            'withdraw_rewards',
            'transfer',
            'message',
            'tx',
            'delegate',
            'redelegate',
            'unbond'
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
msg_attributes_cte AS (
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
        A._inserted_timestamp,
        DENSE_RANK() over(
            PARTITION BY A.tx_id,
            A.msg_group,
            CASE
                WHEN A.msg_type IN(
                    'withdraw_rewards',
                    'message'
                ) THEN TRUE
            END
            ORDER BY
                A.msg_index
        ) dr
    FROM
        msg_attributes_base A
        LEFT JOIN (
            SELECT
                tx_id,
                msg_index
            FROM
                msg_attributes_base
            WHERE
                attribute_value LIKE '%stake'
        ) exc
        ON A.tx_id = exc.tx_id
        AND A.msg_index = exc.msg_index
    WHERE
        exc.tx_id IS NULL
),
block_tx_inserted AS (
    SELECT
        DISTINCT A.tx_id,
        A.block_id,
        A.block_timestamp,
        A.tx_succeeded,
        A._inserted_timestamp
    FROM
        msg_attributes_cte A
),
reward_base AS (
    SELECT
        A.tx_id,
        A.msg_type,
        A.msg_index,
        msg_group,
        dr
    FROM
        msg_attributes_cte A
    WHERE
        msg_type = 'withdraw_rewards'
),
msg_attr_rewards AS (
    SELECT
        A.tx_id,
        A.msg_group,
        A.msg_index,
        A.msg_type,
        A.attribute_key,
        A.attribute_value,
        b.group_id
    FROM
        msg_attributes_cte A
        JOIN (
            SELECT
                DISTINCT tx_id,
                dr,
                msg_index AS group_id,
                msg_group
            FROM
                reward_base
            UNION ALL
            SELECT
                DISTINCT tx_id,
                dr + 1 dr,
                msg_index AS group_id,
                msg_group
            FROM
                reward_base
        ) b
        ON A.tx_id = b.tx_id
        AND A.msg_group = b.msg_group
        AND A.dr = b.dr
    WHERE
        A.msg_type IN(
            'withdraw_rewards',
            'message'
        )
        AND A.msg_group IS NOT NULL
),
reward_combo AS (
    SELECT
        tx_id,
        msg_group,
        group_id,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :validator :: STRING AS validator_address,
        j :amount :: STRING AS amount,
        j :sender :: STRING AS delegator_address
    FROM
        msg_attr_rewards
    WHERE
        attribute_key IN (
            'sender',
            'amount',
            'validator'
        )
        AND NOT (msg_type IN ('transfer', 'redelegate', 'unbond')
        AND attribute_key = 'amount')
        AND NOT (
            msg_type = 'unbond'
            AND attribute_key = 'validator'
        )
    GROUP BY
        tx_id,
        msg_group,
        group_id
),
tran_base AS (
    SELECT
        DISTINCT tx_id,
        msg_group,
        delegator_address
    FROM
        {{ ref('silver__staking') }} A

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
),
tran_tran AS (
    SELECT
        A.tx_id,
        A.msg_type,
        A.msg_index,
        A.msg_group,
        attribute_key,
        attribute_value
    FROM
        msg_attributes_cte A
        INNER JOIN tran_base b
        ON A.tx_id = b.tx_id
        AND A.msg_group = b.msg_group
        AND A.attribute_value = b.delegator_address
        LEFT JOIN (
            SELECT
                DISTINCT tx_id
            FROM
                msg_attributes_cte
            WHERE
                msg_type IN (
                    'withdraw_rewards'
                )
        ) C
        ON A.tx_id = C.tx_id
    WHERE
        A.msg_type = 'transfer'
        AND A.attribute_key = 'recipient'
        AND C.tx_id IS NULL
),
msg_attr_trans AS (
    SELECT
        A.tx_id,
        A.msg_group,
        A.msg_index,
        A.msg_type,
        A.attribute_key,
        A.attribute_value,
        DENSE_RANK() over(
            PARTITION BY A.tx_id,
            A.msg_group
            ORDER BY
                A.msg_index
        ) change_index
    FROM
        tran_tran b
        JOIN msg_attributes_cte A
        ON A.tx_id = b.tx_id
        AND A.msg_group = b.msg_group
    WHERE
        A.msg_type IN (
            'delegate',
            'redelegate',
            'unbond'
        )
        OR (
            A.msg_index = b.msg_index
            AND A.msg_type = 'transfer'
        )
),
tran_combo AS (
    SELECT
        A.tx_id,
        A.msg_group,
        A.msg_index group_id,
        COALESCE(
            b.j :validator :: STRING,
            b.j :source_validator :: STRING
        ) AS validator_address,
        A.j :amount :: STRING AS amount,
        A.j :recipient :: STRING AS delegator_address
    FROM
        (
            SELECT
                tx_id,
                msg_group,
                msg_index,
                change_index + 1 group_id,
                OBJECT_AGG(
                    attribute_key :: STRING,
                    attribute_value :: variant
                ) AS j
            FROM
                (
                    SELECT
                        DISTINCT tx_id,
                        msg_group,
                        msg_index,
                        change_index,
                        attribute_key,
                        attribute_value
                    FROM
                        msg_attr_trans
                    WHERE
                        msg_type = 'transfer'
                        AND attribute_key IN (
                            'amount',
                            'recipient'
                        )
                ) x
            GROUP BY
                tx_id,
                msg_group,
                msg_index,
                group_id
        ) A
        JOIN (
            SELECT
                tx_id,
                msg_group,
                msg_index,
                change_index group_id,
                OBJECT_AGG(
                    attribute_key :: STRING,
                    attribute_value :: variant
                ) AS j
            FROM
                (
                    SELECT
                        DISTINCT tx_id,
                        msg_group,
                        msg_index,
                        change_index,
                        attribute_key,
                        attribute_value
                    FROM
                        msg_attr_trans
                    WHERE
                        msg_type <> 'transfer'
                        AND attribute_key IN (
                            'validator',
                            'source_validator'
                        )
                ) x
            GROUP BY
                tx_id,
                msg_group,
                msg_index,
                group_id
        ) b
        ON A.tx_id = b.tx_id
        AND A.msg_group = b.msg_group
        AND A.group_id = b.group_id
),
combo_all AS (
    SELECT
        tx_id,
        msg_group,
        group_id,
        validator_address,
        amount,
        delegator_address,
        'claim' AS action
    FROM
        tran_combo
    UNION ALL
    SELECT
        tx_id,
        msg_group,
        group_id,
        validator_address,
        amount,
        delegator_address,
        'withdraw_rewards' AS action
    FROM
        reward_combo
),
tx_address AS (
    SELECT
        A.tx_id,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS tx_caller_address,
        SPLIT_PART(
            attribute_value,
            '/',
            1
        ) AS acc_seq_index
    FROM
        msg_attributes_cte A
    WHERE
        attribute_key = 'acc_seq' qualify(ROW_NUMBER() over (PARTITION BY tx_id
    ORDER BY
        acc_seq_index) = 1)
)
SELECT
    b.block_id,
    b.block_timestamp,
    A.tx_id,
    b.tx_succeeded,
    C.tx_caller_address,
    A.action,
    A.msg_group,
    A.delegator_address,
    SUM(
        CASE
            WHEN am.value LIKE '%uaxl' THEN REPLACE(
                am.value,
                'uaxl'
            )
            WHEN am.value LIKE '%ibc%' THEN LEFT(am.value, CHARINDEX('i', am.value) -1)
            ELSE am.value
        END :: INT
    ) AS amount,
    CASE
        WHEN am.value LIKE '%uaxl' THEN 'uaxl'
        WHEN am.value LIKE '%pool%' THEN SUBSTRING(am.value, CHARINDEX('g', am.value), 99)
        WHEN am.value LIKE '%ibc%' THEN SUBSTRING(am.value, CHARINDEX('i', am.value), 99)
        ELSE 'uaxl'
    END AS currency,
    A.validator_address,
    concat_ws(
        '-',
        A.tx_id,
        A.msg_group,
        A.action,
        currency,
        A.delegator_address,
        A.validator_address
    ) AS _unique_key,
    {{ dbt_utils.generate_surrogate_key(
        ['A.tx_id','A.msg_group','A.action','currency','A.delegator_address','A.validator_address']
    ) }} AS staking_rewards_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    b._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    combo_all A
    JOIN LATERAL SPLIT_TO_TABLE(
        A.amount,
        ','
    ) am
    JOIN block_tx_inserted b
    ON A.tx_id = b.tx_id
    JOIN tx_address C
    ON A.tx_id = C.tx_id
GROUP BY
    b.block_id,
    b.block_timestamp,
    A.tx_id,
    b.tx_succeeded,
    C.tx_caller_address,
    A.action,
    A.msg_group,
    A.delegator_address,
    currency,
    A.validator_address,
    b._inserted_timestamp,
    staking_rewards_id
