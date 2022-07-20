
      

  create  table "postgres"._airbyte_test_normalization."types_testing_stg"
  as (
    
with __dbt__cte__types_testing_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "postgres".test_normalization._airbyte_raw_types_testing
select
    jsonb_extract_path_text(_airbyte_data, 'id') as "id",
    jsonb_extract_path_text(_airbyte_data, 'big_integer') as big_integer,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at
from "postgres".test_normalization._airbyte_raw_types_testing as table_alias
-- types_testing
where 1 = 1

),  __dbt__cte__types_testing_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__types_testing_ab1
select
    cast("id" as 
    int
) as "id",
    cast(big_integer as 
    bigint
) as big_integer,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at
from __dbt__cte__types_testing_ab1
-- types_testing
where 1 = 1

)-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__types_testing_ab2
select
    md5(cast(coalesce(cast("id" as text), '') || '-' || coalesce(cast(big_integer as text), '') as text)) as _airbyte_types_testing_hashid,
    tmp.*
from __dbt__cte__types_testing_ab2 tmp
-- types_testing
where 1 = 1

  );
  