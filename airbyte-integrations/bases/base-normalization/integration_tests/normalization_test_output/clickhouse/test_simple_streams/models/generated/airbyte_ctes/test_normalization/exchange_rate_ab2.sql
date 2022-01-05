{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_test_normalization",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('exchange_rate_ab1') }}
select
    accurateCastOrNull(id, '{{ dbt_utils.type_bigint() }}') as id,
    nullif(accurateCastOrNull(trim(BOTH '"' from currency), '{{ dbt_utils.type_string() }}'), 'null') as currency,
    parseDateTimeBestEffortOrNull(trim(BOTH '"' from {{ empty_string_to_null('date') }})) as date,
    parseDateTime64BestEffortOrNull(trim(BOTH '"' from {{ empty_string_to_null('timestamp_col') }})) as timestamp_col,
    accurateCastOrNull({{ quote('HKD@spéçiäl & characters') }}, '{{ dbt_utils.type_float() }}') as {{ quote('HKD@spéçiäl & characters') }},
    nullif(accurateCastOrNull(trim(BOTH '"' from HKD_special___characters), '{{ dbt_utils.type_string() }}'), 'null') as HKD_special___characters,
    accurateCastOrNull(NZD, '{{ dbt_utils.type_float() }}') as NZD,
    accurateCastOrNull(USD, '{{ dbt_utils.type_float() }}') as USD,
    nullif(accurateCastOrNull(trim(BOTH '"' from {{ quote('column`_\'with""_quotes') }}), '{{ dbt_utils.type_string() }}'), 'null') as {{ quote('column`_\'with""_quotes') }},
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('exchange_rate_ab1') }}
-- exchange_rate
where 1 = 1

