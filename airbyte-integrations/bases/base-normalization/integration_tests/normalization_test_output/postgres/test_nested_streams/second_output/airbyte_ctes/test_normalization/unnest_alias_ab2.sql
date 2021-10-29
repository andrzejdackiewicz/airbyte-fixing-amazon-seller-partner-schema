
  create view "postgres"._airbyte_test_normalization."unnest_alias_ab2__dbt_tmp" as (
    
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    cast("id" as 
    bigint
) as "id",
    children,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at
from "postgres"._airbyte_test_normalization."unnest_alias_ab1"
-- unnest_alias
where 1 = 1

  );
