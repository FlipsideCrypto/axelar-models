{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'delete+insert',
    cluster_by = 'block_timestamp::DATE',
) }}

WITH axelar_txs AS (

    SELECT
        DISTINCT tx_id
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        attribute_value IN (
            '/cosmos.bank.v1beta1.MsgSend',
            '/cosmos.bank.v1beta1.MsgMultiSend'
        )

{% if is_incremental() %}
AND _partition_by_block_id >= (
    SELECT
        MAX(_partition_by_block_id) -1
    FROM
        {{ this }}
)
AND _partition_by_block_id <= (
    SELECT
        MAX(_partition_by_block_id) + 10
    FROM
        {{ this }}
)
{% endif %}
),
sender AS (
    SELECT
        tx_id,
        msg_index,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS sender
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type = 'tx'
        AND attribute_key = 'acc_seq'

{% if is_incremental() %}
AND _partition_by_block_id >= (
    SELECT
        MAX(_partition_by_block_id) -1
    FROM
        {{ this }}
)
AND _partition_by_block_id <= (
    SELECT
        MAX(_partition_by_block_id) + 10
    FROM
        {{ this }}
)
{% endif %}
),
msg_index AS (
    SELECT
        v.tx_id,
        attribute_key,
        m.msg_index
    FROM
        axelar_txs v
        LEFT OUTER JOIN {{ ref('silver__msg_attributes') }}
        m
        ON v.tx_id = m.tx_id
        INNER JOIN sender s
        ON v.tx_id = s.tx_id
    WHERE
        msg_type = 'transfer'
        AND attribute_key = 'amount'
        AND m.msg_index > s.msg_index

{% if is_incremental() %}
AND _partition_by_block_id >= (
    SELECT
        MAX(_partition_by_block_id) -1
    FROM
        {{ this }}
)
AND _partition_by_block_id <= (
    SELECT
        MAX(_partition_by_block_id) + 10
    FROM
        {{ this }}
)
{% endif %}
),
receiver AS (
    SELECT
        v.tx_id,
        m.msg_index,
        attribute_value AS receiver
    FROM
        axelar_txs v
        LEFT OUTER JOIN {{ ref('silver__msg_attributes') }}
        m
        ON v.tx_id = m.tx_id
        INNER JOIN sender s
        ON v.tx_id = s.tx_id
    WHERE
        msg_type = 'transfer'
        AND attribute_key = 'recipient'
        AND m.msg_index > s.msg_index

{% if is_incremental() %}
AND _partition_by_block_id >= (
    SELECT
        MAX(_partition_by_block_id) -1
    FROM
        {{ this }}
)
AND _partition_by_block_id <= (
    SELECT
        MAX(_partition_by_block_id) + 10
    FROM
        {{ this }}
)
{% endif %}
),
amount AS (
    SELECT
        v.tx_id,
        m.msg_index,
        COALESCE(
            SPLIT_PART(
                TRIM(
                    REGEXP_REPLACE(
                        attribute_value,
                        '[^[:digit:]]',
                        ' '
                    )
                ),
                ' ',
                0
            ),
            TRY_PARSE_JSON(attribute_value) :amount
        ) AS amount,
        COALESCE(
            RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))),
            TRY_PARSE_JSON(attribute_value) [1] :denom
        ) AS currency
    FROM
        axelar_txs v
        LEFT OUTER JOIN {{ ref('silver__msg_attributes') }}
        m
        ON v.tx_id = m.tx_id
        INNER JOIN sender s
        ON v.tx_id = s.tx_id
    WHERE
        msg_type = 'transfer'
        AND attribute_key = 'amount'
        AND m.msg_index > s.msg_index

{% if is_incremental() %}
AND _partition_by_block_id >= (
    SELECT
        MAX(_partition_by_block_id) -1
    FROM
        {{ this }}
)
AND _partition_by_block_id <= (
    SELECT
        MAX(_partition_by_block_id) + 10
    FROM
        {{ this }}
)
{% endif %}
),

axelar_txs_final as (
SELECT
    block_id,
    block_timestamp,
    t.blockchain,
    chain_id,
    r.tx_id,
    tx_succeeded,
    'AXELAR' AS transfer_type,
    r.msg_index,
    sender,
    amount,
    currency,
    COALESCE(
        l.raw_metadata [1] :exponent,
        6
    ) AS DECIMAL,
    receiver,
    _partition_by_block_id,
    concat_ws(
        '-',
        r.tx_id,
        r.msg_index,
        currency
    ) AS _unique_key
FROM
    receiver r
    LEFT OUTER JOIN amount C
    ON r.tx_id = C.tx_id
    AND r.msg_index = C.msg_index
    LEFT OUTER JOIN sender s
    ON r.tx_id = s.tx_id
    LEFT OUTER JOIN {{ ref('silver__transactions') }}
    t
    ON r.tx_id = t.tx_id
    LEFT OUTER JOIN {{ ref('core__dim_labels') }}
    l
    ON currency = l.address

{% if is_incremental() %}
WHERE
    _partition_by_block_id >= (
        SELECT
            MAX(_partition_by_block_id) -1
        FROM
            {{ this }}
    )
    AND _partition_by_block_id <= (
        SELECT
            MAX(_partition_by_block_id) + 10
        FROM
            {{ this }}
    )
{% endif %}
),

    ibc_in_tx as (
    select 
    block_id, 
    block_timestamp,
    blockchain,
    chain_id,
    tx_id, 
    tx_succeeded,
    msg_index,
    'IBC_TRANSFER_IN' as transfer_type,
    
    try_parse_json(attribute_value):sender::string as sender,
    try_parse_json(attribute_value):amount::int as amount,
    case
        when try_parse_json(attribute_value):denom::string like '%/%'
                then split(try_parse_json(attribute_value):denom::string, '/')
                            [array_size(split(try_parse_json(attribute_value):denom::string, '/')) - 1]
        else try_parse_json(attribute_value):denom::string 
        end as currency,
    
    try_parse_json(attribute_value):receiver::string as receiver,

    _partition_by_block_id,
    concat_ws(
        '-',
        tx_id,
        msg_index,
        currency
    ) AS _unique_key
    
    from {{ ref('silver__msg_attributes') }}
    where msg_type = 'write_acknowledgement'
    and attribute_key = 'packet_data'
    and try_parse_json(attribute_value): amount is not null 

{% if is_incremental() %}
AND
    _partition_by_block_id >= (
        SELECT
            MAX(_partition_by_block_id) -1
        FROM
            {{ this }}
    )
    AND _partition_by_block_id <= (
        SELECT
            MAX(_partition_by_block_id) + 10
        FROM
            {{ this }}
    )
{% endif %}

    ),

    ibc_out_txid as (
    select 
    tx_id 
    from {{ ref('silver__msg_attributes') }}
    where msg_type = 'ibc_transfer'
    
    {% if is_incremental() %}
AND
    _partition_by_block_id >= (
        SELECT
            MAX(_partition_by_block_id) -1
        FROM
            {{ this }}
    )
    AND _partition_by_block_id <= (
        SELECT
            MAX(_partition_by_block_id) + 10
        FROM
            {{ this }}
    )
{% endif %}
    ),

    ibc_out_tx as (
    select 
    block_id, 
    block_timestamp,
    blockchain,
    chain_id,
    tx_id, 
    tx_succeeded,
    msg_index,
    'IBC_TRANSFER_OUT' as transfer_type,

    try_parse_json(attribute_value):sender::string as sender,
    try_parse_json(attribute_value):amount::int as amount,
    case
        when try_parse_json(attribute_value):denom::string like '%/%'
                then split(try_parse_json(attribute_value):denom::string, '/')
                            [array_size(split(try_parse_json(attribute_value):denom::string, '/')) - 1]
        else try_parse_json(attribute_value):denom::string 
        end as currency,
    
    try_parse_json(attribute_value):receiver::string as receiver,
    _partition_by_block_id,
    concat_ws(
        '-',
        tx_id,
        msg_index,
        currency
    ) AS _unique_key
    from {{ ref('silver__msg_attributes') }}
    where tx_id in (select tx_id from ibc_out_txid)
    and msg_type = 'send_packet'
    and attribute_key = 'packet_data'

    {% if is_incremental() %}
AND
    _partition_by_block_id >= (
        SELECT
            MAX(_partition_by_block_id) -1
        FROM
            {{ this }}
    )
    AND _partition_by_block_id <= (
        SELECT
            MAX(_partition_by_block_id) + 10
        FROM
            {{ this }}
    )
{% endif %}
    ),     
    
decimals as (
    select*,
    coalesce (raw_metadata[0]:aliases[0]::string, 
                raw_metadata[1]:denom) 
                    as denom_name ,
    case 
        when raw_metadata[0]:account_address is not null then null
        else COALESCE( raw_metadata [1] :exponent::int, 6) 
        end AS DECIMAL
    
    from {{ ref('core__dim_labels') }}
    ),

    ibc_transfers_agg as (
        select * from ibc_out_tx
        union all 
        select * from ibc_in_tx
    ),

ibc_tx_final as (
    select 
    i.block_id, 
    i.block_timestamp,
    i.blockchain,
    i.chain_id,
    i.tx_id, 
    i.tx_succeeded,
    i.transfer_type,
    i.sender,
    i.amount,
    i.currency,
    case 
        when i.currency in ( 'uusd', 'uosmo') then 6
        when i.currency like '%-wei' then 18 
        else d.decimal
        end as decimal,
    i.receiver,
    msg_index,
    _partition_by_block_id,
    _unique_key
    from ibc_transfers_agg i 
    left join decimals d  on i.currency = d.denom_name 
    )
    
    select 
    block_id, 
    block_timestamp,
    blockchain,
    chain_id,
    tx_id, 
    tx_succeeded,
    transfer_type,
    sender, 
    amount, 
    currency, 
    decimal,
    receiver,
    msg_index,
    _partition_by_block_id,
    _unique_key
    from ibc_tx_final 
    
    union all 

    select 
    block_id, 
    block_timestamp,
    blockchain,
    chain_id,
    tx_id, 
    tx_succeeded,
    transfer_type,
    sender, 
    amount, 
    currency, 
    decimal,
    receiver,
    msg_index,
    _partition_by_block_id,
    _unique_key
    from axelar_txs_final
    

