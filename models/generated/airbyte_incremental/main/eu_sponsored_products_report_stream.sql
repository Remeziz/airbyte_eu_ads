{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "main",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('eu_sponsored_products_report_stream_ab3') }}
select
    replace(json_extract(metric, '$."sku"') , '"', '') as sku,
    replace(json_extract(metric, '$."asin"') , '"', '') as asin,
    (replace(json_extract(metric, '$."cost"') , '"', '')) as cost,
    (replace(json_extract(metric, '$."impressions"') , '"', '')) as impressions,
    (replace(json_extract(metric, '$."clicks"') , '"', '')) as clicks,
    (replace(json_extract(metric, '$."attributedSales1d"') , '"', '')) as sales,
    (replace(json_extract(metric, '$."attributedUnitsOrdered1d"') , '"', '')) as orders,
    profileid,
    updatedat,
    recordtype,
    reportdate,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_eu_sponsored____report_stream_hashid
from {{ ref('eu_sponsored_products_report_stream_ab3') }}
-- eusponsored_products_report_stream from {{ source('main', '_airbyte_raw_eu_sponso__roducts_report_stream') }}
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

