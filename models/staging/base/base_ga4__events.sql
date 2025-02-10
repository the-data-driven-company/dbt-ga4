{% set partitions_to_replace = ['current_date'] %}
{% for i in range(var('static_incremental_days')) %}
    {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
{% endfor %}

{{
    config(
        pre_hook="{{ ga4.combine_property_data() }}" if var('combined_dataset', false) else "",
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by={
            "field": "event_date_dt",
            "data_type": "date",
        },
        partitions = partitions_to_replace,
        cluster_by=['event_name']
    )
}}

with 
source as (
    select _table_suffix 
    from {{ source('ga4', 'events') }})
    where (_table_suffix not like '%\\_%' or _table_suffix like '%fresh\\_%'
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and parse_date('%Y%m%d', right(_table_suffix, 8)) in ({{ partitions_to_replace | join(',') }})
    {% endif %}
),

standard_partitions as (
    select distinct _table_suffix
    from source
    where 
        _table_suffix not like '%\\_%'
        and cast(right(_table_suffix, 8) as int64) >= {{var('start_date')}}
),

fresh_partitions as (
    select distinct _table_suffix, cast(right(_table_suffix, 8) as int64) as date_int
    from source
    where 
        _table_suffix like '%fresh_%'
        and cast(right(_table_suffix, 8) as int64) > (select cast(max(_table_suffix) as int64) from standard_partitions)
),

clean_table_suffix_list as (
        select _table_suffix from standard_partitions
            union distinct
        select _table_suffix from fresh_partitions
),

source_deduped as (
    select
        {{ ga4.base_select_source() }}
    from {{ source('ga4', 'events') }}
    where _table_suffix in (
        select _table_suffix from clean_table_suffix_list
    )
),

renamed as (
    select
        {{ ga4.base_select_renamed() }}
    from source_deduped
),

final as (
    select * from renamed
    qualify row_number() over(partition by event_date_dt, stream_id, user_pseudo_id, session_id, event_name, event_timestamp, to_json_string(ARRAY(SELECT params FROM UNNEST(event_params) AS params ORDER BY key))) = 1
)

select * from final