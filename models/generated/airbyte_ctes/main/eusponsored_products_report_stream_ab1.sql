{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_main",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('main', '_airbyte_raw_eu_sponso__roducts_report_stream') }}
select
    {{ json_extract('table_alias', '_airbyte_data', ['metric'], ['metric']) }} as metric,
    {{ json_extract_scalar('_airbyte_data', ['profileId'], ['profileId']) }} as profileid,
    {{ json_extract_scalar('_airbyte_data', ['updatedAt'], ['updatedAt']) }} as updatedat,
    {{ json_extract_scalar('_airbyte_data', ['recordType'], ['recordType']) }} as recordtype,
    {{ json_extract_scalar('_airbyte_data', ['reportDate'], ['reportDate']) }} as reportdate,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('main', '_airbyte_raw_eu_sponso__roducts_report_stream') }} as table_alias
-- eu_sponsored_products_report_stream
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

