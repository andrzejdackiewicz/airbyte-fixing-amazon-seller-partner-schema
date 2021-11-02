{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = env_var('AIRBYTE_DEFAULT_UNIQUE_KEY', '_AIRBYTE_AB_ID'),
    schema = "_AIRBYTE_TEST_NORMALIZATION",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    _AIRBYTE_UNNEST_ALIAS_HASHID,
    cast(AB_ID as {{ dbt_utils.type_bigint() }}) as AB_ID,
    cast(OWNER as {{ type_json() }}) as OWNER,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('UNNEST_ALIAS_CHILDREN_AB1') }}
-- CHILDREN at unnest_alias/children
where 1 = 1

