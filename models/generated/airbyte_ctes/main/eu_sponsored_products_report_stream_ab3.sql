{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_main",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('eu_sponsored_products_report_stream_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        object_to_string('metric'),
        'profileid',
        'updatedat',
        'recordtype',
        'reportdate',
    ]) }} as _airbyte_eu_sponsored___report_stream_hashid,
    tmp.*
from {{ ref('eu_sponsored_products_report_stream_ab2') }} tmp
-- eu_sponsored_products_report_stream
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

