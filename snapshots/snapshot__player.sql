{% snapshot snapshot_players %}

{{
    config(
        target_schema='snapshots', 
        unique_key='player_id',  
        strategy='timestamp', 
        updated_at='updated_at' 
    )
}}

select 
    event.value:statistics.totals.competitors.players.id::string as player_id,
    event.value:statistics.totals.competitors.players.name::string as player_name,
    event.value:statistics.totals.competitors.players.country::string as country,
    event.value:statistics.totals.competitors.players.type::string as type,
    updated_at
from {{ source('liverpool', 'sportradar_data') }},
lateral flatten(input => data:summaries) event

{% endsnapshot %}