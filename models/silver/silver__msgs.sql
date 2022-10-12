
{{ config(
  materialized = 'incremental',
  unique_key = "_unique_key",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
) }}

with base as ( 
    
    select
t.block_id, 
t.block_timestamp,
t.tx_id,
t.blockchain,
t.chain_id,
t.gas_used, 
t.gas_wanted, 
t.tx_succeeded, 

f.value as msg,
f.index::int as msg_index,
msg:type::string as msg_type,



iff( try_base64_decode_string( msg:attributes[0]:key::string) = 'action' ,
        TRUE, 
        FALSE
    ) as is_action, 

nullif(
      (
      conditional_true_event(is_action) over (PARTITION BY tx_id ORDER BY msg_index asc) 
        -1
      ),
        -1
    ) as msg_group,

iff( try_base64_decode_string( msg:attributes[0]:key::string) = 'module' ,
        TRUE, 
        FALSE
    ) as is_module,



try_base64_decode_string( msg:attributes[0]:key::string) as attribute_key, 

try_base64_decode_string( msg:attributes[0]:value::string) as attribute_value,

t._partition_by_block_id
  
    
from  {{ ref('silver__transactions') }} t, lateral flatten(input => msgs) f

{% if is_incremental() %}
WHERE _partition_by_block_id >= (
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
{% else %}
    WHERE _partition_by_block_id IN (
        0,
        1
    )
{% endif %}

 ),
 
 exec_actions as (
 select 
    distinct tx_id, 
    msg_group 
 from base 
     where msg_type = 'message'
     and attribute_key = 'action'
     and lower(attribute_value) like '%exec%' -- there are none currently 
 ),
 
 grouping as (
 select 
    base.tx_id,
    base.msg_index,
    RANK() over( PARTITION BY base.tx_id, base.msg_group ORDER BY base.msg_index) 
        -1 as msg_sub_group
  from base 
        inner join exec_actions e on base.tx_id = e.tx_id
                                and base.msg_group = e.msg_group 
    
  where base.is_module = 'TRUE'
     and base.msg_type = 'message'
),

final as (
SELECT
  block_id,
  block_timestamp,
  blockchain,
  chain_id,
  a.tx_id,
  tx_succeeded,
  msg_group,
  CASE
    WHEN msg_group IS NULL THEN NULL
    ELSE COALESCE(
      LAST_VALUE(
        b.msg_sub_group ignore nulls
      ) over(
        PARTITION BY a.tx_id,
        msg_group
        ORDER BY
          a.msg_index DESC rows unbounded preceding
      ),
      0
    )
  END AS msg_sub_group,
  a.msg_index,
  msg_type,
  msg,
  concat_ws(
    '-',
    a.tx_id,
    a.msg_index
  ) AS _unique_key,
  _partition_by_block_id
FROM base a
  LEFT JOIN grouping b ON a.tx_id = b.tx_id
  AND A.msg_index = b.msg_index
    )
    
select 
    block_id,
    block_timestamp,
    blockchain,
    chain_id,
    tx_id,
    tx_succeeded,
    msg_group,
    msg_sub_group,
    msg_index,
    msg_type,
    msg,
    _unique_key,
    _partition_by_block_id
from final 

