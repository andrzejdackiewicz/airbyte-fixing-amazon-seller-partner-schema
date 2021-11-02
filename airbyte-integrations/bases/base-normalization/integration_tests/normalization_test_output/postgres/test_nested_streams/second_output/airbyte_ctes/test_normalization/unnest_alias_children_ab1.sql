
  create view "postgres"._airbyte_test_normalization."unnest_alias_children_ab1__dbt_tmp" as (
    
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema

select
    _airbyte_unnest_alias_hashid,
    jsonb_extract_path_text(_airbyte_nested_data, 'ab_id') as ab_id,
    
        jsonb_extract_path(_airbyte_nested_data, 'owner')
     as "owner",
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at
from "postgres".test_normalization."unnest_alias" as table_alias
-- children at unnest_alias/children
cross join jsonb_array_elements(
        case jsonb_typeof(children)
        when 'array' then children
        else '[]' end
    ) as _airbyte_nested_data
where 1 = 1
and children is not null
  );
