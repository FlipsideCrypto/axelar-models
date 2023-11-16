{{ config(
    materialized = 'incremental',
    unique_key = ['tx_id','proposal_id','voter'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['noncore']
) }}

WITH base_atts AS (

    SELECT
        block_id,
        block_timestamp,
        tx_succeeded,
        tx_id,
        msg_group,
        msg_sub_group,
        msg_index,
        COALESCE(
            b.key,
            attribute_key
        ) AS attribute_key,
        COALESCE(
            b.value,
            attribute_value
        ) AS attribute_value,
        msg_type,
        _inserted_timestamp
    FROM
        {{ ref('silver__msg_attributes') }},
        LATERAL FLATTEN(TRY_PARSE_JSON(attribute_value), outer => TRUE) b
    WHERE
        (
            msg_type = 'proposal_vote'
            OR (
                msg_type = 'message'
                AND attribute_key = 'sender'
            )
        )
        AND tx_id <> '6FC4E1A49A885B9DD006C702413BD2F48209461982A62239F6C75925AA54D80D'

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
fin AS (
    SELECT
        block_id,
        block_timestamp,
        tx_succeeded,
        tx_id,
        msg_group,
        msg_sub_group,
        _inserted_timestamp,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :proposal_id :: INT AS proposal_id,
        j :option :: STRING AS vote_option,
        j :weight :: NUMBER AS vote_weight
    FROM
        base_atts
    WHERE
        msg_type = 'proposal_vote'
    GROUP BY
        block_id,
        block_timestamp,
        tx_succeeded,
        tx_id,
        msg_group,
        msg_sub_group,
        _inserted_timestamp
)
SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_id,
    A.tx_succeeded,
    b.voter,
    A.proposal_id,
    CASE
        WHEN A.vote_option = 'VOTE_OPTION_YES' THEN 1
        WHEN A.vote_option = 'VOTE_OPTION_ABSTAIN' THEN 2
        WHEN A.vote_option = 'VOTE_OPTION_NO' THEN 3
        WHEN A.vote_option = 'VOTE_OPTION_NO_WITH_VETO' THEN 4
        ELSE A.vote_option :: INT
    END AS vote_option,
    A.vote_weight,
    {{ dbt_utils.generate_surrogate_key(
        ['a.tx_id','a.proposal_id','b.voter']
    ) }} AS governance_votes_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    A._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    fin A
    LEFT JOIN (
        SELECT
            tx_id,
            msg_group,
            msg_sub_group,
            attribute_value AS voter
        FROM
            base_atts
        WHERE
            msg_type = 'message'
            AND attribute_key = 'sender'
    ) b
    ON A.tx_id = b.tx_id
    AND A.msg_group = b.msg_group
    AND A.msg_sub_group = b.msg_sub_group
