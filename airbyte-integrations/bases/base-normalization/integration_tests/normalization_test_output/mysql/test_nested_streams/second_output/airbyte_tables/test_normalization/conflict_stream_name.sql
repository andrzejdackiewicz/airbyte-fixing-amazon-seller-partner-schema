

  create  table
    test_normalization.`conflict_stream_name__dbt_tmp`
  as (
    
-- Final base SQL model
select
    id,
    conflict_stream_name,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    
    CURRENT_TIMESTAMP
 as _airbyte_normalized_at,
    _airbyte_conflict_stream_name_hashid
from _airbyte_test_normalization.`conflict_stream_name_ab3`
-- conflict_stream_name from test_normalization._airbyte_raw_conflict_stream_name
where 1 = 1
  )
