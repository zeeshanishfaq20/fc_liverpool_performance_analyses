{% snapshot snapshot_sport_event %}

{{
    config(
        target_schema='snapshots', 
        unique_key='event_id',  
        strategy='timestamp', 
        updated_at='updated_at' 
    )
}}

select
    event.value:sport_event.id::string as event_id,
    event.value:sport_event.start_time::timestamp_ntz as start_time, 
    updated_at::timestamp as updated_at
from {{ source('liverpool', 'sportradar_data') }},
lateral flatten(input => data:summaries) event

{% endsnapshot %}