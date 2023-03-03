{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_main",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('eusponsored_products_report_stream_ab1') }}
select
    cast(metric as {{ type_json() }}) as metric,
    cast(profileid as {{ dbt_utils.type_bigint() }}) as profileid,
    cast({{ empty_string_to_null('updatedat') }} as {{ type_timestamp_with_timezone() }}) as updatedat,
    cast(recordtype as {{ dbt_utils.type_string() }}(1024)) as recordtype,
    cast(reportdate as {{ dbt_utils.type_string() }}(1024)) as reportdate,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('eusponsored_products_report_stream_ab1') }}
-- eusponsored_products_report_stream
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

