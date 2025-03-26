{% snapshot snapshot_referee %}

{{
    config(
        target_schema='snapshots', 
        unique_key='referee_id',  
        strategy='timestamp', 
        updated_at='updated_at' 
    )
}}

select
    referee.value:id::string as referee_id,
    referee.value:name::string as referee_name,
    referee.value:nationality::string as nationality,
    referee.value:country_code::string as country_code,
    referee.value:type::string as type,
    updated_at::timestamp as updated_at
from {{ source('liverpool', 'sportradar_data') }},
lateral flatten(input => data:summaries) event,
lateral flatten(input => event.value:sport_event.sport_event_conditions.referees) referee

{% endsnapshot %}