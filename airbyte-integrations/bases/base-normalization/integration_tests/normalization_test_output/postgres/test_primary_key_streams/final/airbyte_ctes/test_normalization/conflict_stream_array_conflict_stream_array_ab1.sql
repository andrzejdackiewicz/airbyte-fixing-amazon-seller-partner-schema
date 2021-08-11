
  create view "postgres"._airbyte_test_normalization."conflict_stream_array_conflict_stream_array_ab1__dbt_tmp" as (
    
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    _airbyte_conflict_stream_array_hashid,
    jsonb_extract_path(conflict_stream_array, 'conflict_stream_name') as conflict_stream_name,
    _airbyte_emitted_at
from "postgres".test_normalization."conflict_stream_array" as table_alias
where conflict_stream_array is not null
-- conflict_stream_array at conflict_stream_array/conflict_stream_array
  );
