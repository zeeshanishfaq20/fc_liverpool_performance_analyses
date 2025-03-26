{% snapshot snapshot_competitor %}

{{
    config(
        target_schema='snapshots', 
        unique_key='competitor_id',  
        strategy='timestamp', 
        updated_at='updated_at' 
    )
}}

select 
    competitor.value:id::string as competitor_id,
    competitor.value:name::string as competitor_name,
    competitor.value:country::string as country,
    competitor.value:type::string as type,
    updated_at
from {{ source('liverpool', 'sportradar_data') }},
lateral flatten(input => data:summaries) event,
lateral flatten(input => event.value:sport_event.competitors) competitor

{% endsnapshot %}