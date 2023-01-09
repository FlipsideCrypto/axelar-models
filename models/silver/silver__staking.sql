{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH att_base AS (

    SELECT
        A.tx_id,
        A.block_id,
        A.block_timestamp,
        A.tx_succeeded,
        A.attribute_key,
        A.attribute_value,
        A.msg_index,
        A.msg_type,
        A.msg_group,
        A._inserted_timestampa.
    FROM
        {{ ref('silver__msg_attributes') }} A
    WHERE
        (
            msg_type IN (
                'delegate',
                'redelegate',
                'unbond',
                'message'
            )
            OR attribute_key = 'acc_seq'
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
base AS (
    SELECT
        A.tx_id,
        A.block_id,
        A.block_timestamp,
        A.tx_succeeded,
        A.attribute_key,
        A.attribute_value,
        A.msg_index,
        A.msg_type,
        A.msg_group,
        _inserted_timestamp
    FROM
        att_base A
    WHERE
        msg_type IN (
            'delegate',
            'redelegate',
            'unbond'
        )
),
msg_attr AS (
    SELECT
        A.tx_id,
        A.block_id,
        A.block_timestamp,
        A.tx_succeeded,
        A.attribute_key,
        A.attribute_value,
        A.msg_index,
        A.msg_type,
        A.msg_group,
        _inserted_timestamp
    FROM
        att_base A
        JOIN (
            SELECT
                DISTINCT tx_id,
                msg_index
            FROM
                base
            UNION ALL
            SELECT
                DISTINCT tx_id,
                msg_index + 1 msg_index
            FROM
                base
        ) b
        ON A.tx_id = b.tx_id
        AND A.msg_index = b.msg_index
    WHERE
        A.msg_type IN (
            'delegate',
            'message',
            'redelegate',
            'unbond'
        )
),
tx_address AS (
    SELECT
        A.tx_id,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        SPLIT_PART(
            j :acc_seq :: STRING,
            '/',
            0
        ) AS tx_caller_address
    FROM
        att_base A
    WHERE
        attribute_key = 'acc_seq'
    GROUP BY
        A.tx_id,
        msg_group
),
valid AS (
    SELECT
        tx_id,
        msg_group,
        msg_index,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        COALESCE(
            j :validator :: STRING,
            j :destination_validator :: STRING
        ) AS validator_address,
        j :source_validator :: STRING AS redelegate_source_validator_address
    FROM
        msg_attr
    WHERE
        attribute_key LIKE '%validator'
    GROUP BY
        tx_id,
        msg_group,
        msg_index
),
sendr AS (
    SELECT
        tx_id,
        msg_group,
        msg_index,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :sender :: STRING AS sender
    FROM
        msg_attr A
    WHERE
        attribute_key = 'sender'
    GROUP BY
        tx_id,
        msg_group,
        msg_index
),
amount AS (
    SELECT
        tx_id,
        msg_group,
        msg_index,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :amount :: STRING AS amount
    FROM
        msg_attr
    WHERE
        attribute_key = 'amount'
    GROUP BY
        tx_id,
        msg_group,
        msg_index
),
ctime AS (
    SELECT
        tx_id,
        msg_group,
        msg_index,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :completion_time :: STRING AS completion_time
    FROM
        msg_attr
    WHERE
        attribute_key = 'completion_time'
    GROUP BY
        tx_id,
        msg_group,
        msg_index
),
prefinal AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.tx_id,
        A.tx_succeeded,
        A.msg_group,
        b.sender AS delegator_address,
        d.amount,
        A.msg_type AS action,
        C.validator_address,
        C.redelegate_source_validator_address,
        e.completion_time,
        A._inserted_timestamp
    FROM
        (
            SELECT
                DISTINCT block_id,
                block_timestamp,
                tx_id,
                tx_succeeded,
                msg_group,
                msg_index,
                REPLACE(
                    msg_type,
                    'unbond',
                    'undelegate'
                ) msg_type,
                _inserted_timestamp
            FROM
                base
        ) A
        JOIN sendr b
        ON A.tx_id = b.tx_id
        AND A.msg_group = b.msg_group
        AND A.msg_index + 1 = b.msg_index
        JOIN valid C
        ON A.tx_id = C.tx_id
        AND A.msg_group = C.msg_group
        AND A.msg_index = C.msg_index
        JOIN amount d
        ON A.tx_id = d.tx_id
        AND A.msg_group = d.msg_group
        AND A.msg_index = d.msg_index
        LEFT JOIN ctime e
        ON A.tx_id = e.tx_id
        AND A.msg_group = e.msg_group
        AND A.msg_index = e.msg_index
)
SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_id,
    A.tx_succeeded,
    C.tx_caller_address,
    A.action,
    A.delegator_address,
    A.validator_address,
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
    A.redelegate_source_validator_address,
    A.completion_time :: datetime completion_time,
    A.msg_group,
    A._inserted_timestamp,
    concat_ws(
        '-',
        A.tx_id,
        A.msg_group,
        action,
        currency,
        delegator_address,
        validator_address
    ) AS _unique_key
FROM
    prefinal A
    JOIN LATERAL SPLIT_TO_TABLE(
        A.amount,
        ','
    ) am
    JOIN tx_address C
    ON A.tx_id = C.tx_id
GROUP BY
    A.block_id,
    A.block_timestamp,
    A.tx_id,
    A.tx_succeeded,
    C.tx_caller_address,
    A.action,
    A.msg_group,
    A.delegator_address,
    currency,
    A.validator_address,
    A.redelegate_source_validator_address,
    completion_time,
    A._inserted_timestamp,
    _unique_key
