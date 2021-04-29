{{ config(alias="exchange_rate_ab2", schema="_airbyte_test_normalization", tags=["top-level-intermediate"]) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    cast(id as {{ dbt_utils.type_bigint() }}) as id,
    cast(currency as {{ dbt_utils.type_string() }}) as currency,
    cast(date as {{ dbt_utils.type_string() }}) as date,
    cast({{ adapter.quote('hkd@spéçiäl & characters') }} as {{ dbt_utils.type_float() }}) as {{ adapter.quote('hkd@spéçiäl & characters') }},
    cast(nzd as {{ dbt_utils.type_float() }}) as nzd,
    cast(usd as {{ dbt_utils.type_float() }}) as usd,
    _airbyte_emitted_at
from {{ ref('exchange_rate_ab1_e8c') }}
-- exchange_rate

