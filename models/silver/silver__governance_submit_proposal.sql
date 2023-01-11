{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

    SELECT
        tx_id,
        block_id,
        block_timestamp,
        tx_succeeded,
        msg_type,
        attribute_key,
        attribute_value,
        _inserted_timestamp
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        (
            (
                msg_type = 'submit_proposal'
                AND attribute_key IN (
                    'proposal_id',
                    'proposal_type'
                )
            )
            OR attribute_key = 'acc_seq'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) _inserted_timestamp
    FROM
        {{ this }}
)
{% endif %}
),
proposal_ids AS (
    SELECT
        tx_id,
        block_id,
        block_timestamp,
        tx_succeeded,
        attribute_value AS proposal_id,
        _inserted_timestamp
    FROM
        base
    WHERE
        msg_type = 'submit_proposal'
        AND attribute_key = 'proposal_id'
),
proposal_type AS (
    SELECT
        tx_id,
        attribute_value AS proposal_type
    FROM
        base
    WHERE
        msg_type = 'submit_proposal'
        AND attribute_key = 'proposal_type'
),
proposer AS (
    SELECT
        tx_id,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS proposer
    FROM
        base
    WHERE
        attribute_key = 'acc_seq'
)
SELECT
    block_id,
    block_timestamp,
    p.tx_id,
    tx_succeeded,
    proposer,
    p.proposal_id :: NUMBER AS proposal_id,
    y.proposal_type,
    _inserted_timestamp
FROM
    proposal_ids p
    INNER JOIN proposal_type y
    ON p.tx_id = y.tx_id
    INNER JOIN proposer pp
    ON p.tx_id = pp.tx_id
