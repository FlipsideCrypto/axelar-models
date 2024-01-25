{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['noncore']
) }}

WITH

{% if is_incremental() %}
max_date AS (

    SELECT
        MAX(
            _inserted_timestamp
        ) _inserted_timestamp
    FROM
        {{ this }}
),
{% endif %}

base AS (
    SELECT
        tx_id,
        block_id,
        block_timestamp,
        tx_succeeded,
        msg_type,
        msg_index,
        attribute_key,
        attribute_value,
        _inserted_timestamp,
        attribute_value AS proposal_id
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        (
            msg_type = 'submit_proposal'
            AND attribute_key = 'proposal_id'
        )
        OR (
            msg_type = 'submit_proposal'
            AND attribute_key = 'proposal_type'
        )
        OR (
            attribute_key = 'acc_seq'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        max_date
)
{% endif %}
),
proposal_id AS (
    SELECT
    tx_id,
    block_id,
    block_timestamp,
    tx_succeeded,
    msg_type,
    msg_index,
    attribute_key,
    attribute_value,
    _inserted_timestamp,
    attribute_value AS proposal_id
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

qualify(ROW_NUMBER() over(PARTITION BY tx_id
ORDER BY
    msg_index)) = 1
)
SELECT
    block_id,
    block_timestamp,
    p.tx_id,
    tx_succeeded,
    proposer,
    p.proposal_id :: NUMBER AS proposal_id,
    y.proposal_type,
{{ dbt_utils.generate_surrogate_key(
    ['p.tx_id']
) }} AS governance_submit_proposal_id,
SYSDATE() AS inserted_timestamp,
SYSDATE() AS modified_timestamp,
_inserted_timestamp,
'{{ invocation_id }}' AS _invocation_id
FROM
    proposal_id p
    INNER JOIN proposal_type y
    ON p.tx_id = y.tx_id
    INNER JOIN proposer pp
    ON p.tx_id = pp.tx_id