

  create view "integrationtests"._airbyte_test_normalization."_airbyte_test_normalization_dedup_exchange_rate_ab3__dbt_tmp" as (
    
-- SQL model to build a hash column based on the values of this record
select
    *,
    md5(cast(
    
    coalesce(cast(id as varchar), '') || '-' || coalesce(cast(currency as varchar), '') || '-' || coalesce(cast(date as varchar), '') || '-' || coalesce(cast(hkd as varchar), '') || '-' || coalesce(cast(nzd as varchar), '') || '-' || coalesce(cast(usd as varchar), '')

 as varchar)) as _airbyte_dedup_exchange_rate_hashid
from "integrationtests"._airbyte_test_normalization."dedup_exchange_rate_ab2"
-- dedup_exchange_rate
  ) ;
