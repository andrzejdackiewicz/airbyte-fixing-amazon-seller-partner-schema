

  create view "integrationtests"._airbyte_test_normalization."dedup_exchange_rate_stg__dbt_tmp" as (
    
with __dbt__cte__dedup_exchange_rate_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "integrationtests".test_normalization._airbyte_raw_dedup_exchange_rate
select
    case when _airbyte_data."id" != '' then _airbyte_data."id" end as id,
    case when _airbyte_data."currency" != '' then _airbyte_data."currency" end as currency,
    case when _airbyte_data."date" != '' then _airbyte_data."date" end as date,
    case when _airbyte_data."timestamp_col" != '' then _airbyte_data."timestamp_col" end as timestamp_col,
    case when _airbyte_data."HKD@spéçiäl & characters" != '' then _airbyte_data."HKD@spéçiäl & characters" end as "hkd@spéçiäl & characters",
    case when _airbyte_data."HKD_special___characters" != '' then _airbyte_data."HKD_special___characters" end as hkd_special___characters,
    case when _airbyte_data."NZD" != '' then _airbyte_data."NZD" end as nzd,
    case when _airbyte_data."USD" != '' then _airbyte_data."USD" end as usd,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    getdate() as _airbyte_normalized_at
from "integrationtests".test_normalization._airbyte_raw_dedup_exchange_rate as table_alias
-- dedup_exchange_rate
where 1 = 1

),  __dbt__cte__dedup_exchange_rate_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__dedup_exchange_rate_ab1
select
    cast(id as 
    bigint
) as id,
    cast(currency as varchar) as currency,
    cast(nullif(date::varchar, '') as 
    date
) as date,
    cast(nullif(timestamp_col::varchar, '') as 
    timestamp with time zone
) as timestamp_col,
    cast("hkd@spéçiäl & characters" as 
    float
) as "hkd@spéçiäl & characters",
    cast(hkd_special___characters as varchar) as hkd_special___characters,
    cast(nzd as 
    float
) as nzd,
    cast(usd as 
    float
) as usd,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    getdate() as _airbyte_normalized_at
from __dbt__cte__dedup_exchange_rate_ab1
-- dedup_exchange_rate
where 1 = 1

)-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__dedup_exchange_rate_ab2
select
    md5(cast(coalesce(cast(id as varchar), '') || '-' || coalesce(cast(currency as varchar), '') || '-' || coalesce(cast(date as varchar), '') || '-' || coalesce(cast(timestamp_col as varchar), '') || '-' || coalesce(cast("hkd@spéçiäl & characters" as varchar), '') || '-' || coalesce(cast(hkd_special___characters as varchar), '') || '-' || coalesce(cast(nzd as varchar), '') || '-' || coalesce(cast(usd as varchar), '') as varchar)) as _airbyte_dedup_exchange_rate_hashid,
    tmp.*
from __dbt__cte__dedup_exchange_rate_ab2 tmp
-- dedup_exchange_rate
where 1 = 1

  ) ;
