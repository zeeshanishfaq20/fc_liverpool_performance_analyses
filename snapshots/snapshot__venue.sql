{% snapshot snapshot_venue %}

{{
    config(
        target_schema='snapshots', 
        unique_key='venue_id',  
        strategy='timestamp', 
        updated_at='updated_at' 
    )
}}

select 
        event.value:sport_event.venue.id::string as venue_id,
        event.value:sport_event.venue.name::string as venue_name,
        event.value:sport_event.venue.city_name::string as city_name,
        event.value:sport_event.venue.country_name::string as country_name,
        updated_at::timestamp as updated_at
    from {{ source('liverpool', 'sportradar_data') }},
    lateral flatten(input => data:summaries) event

{% endsnapshot %}