{{ config(
    materialized='incremental',
    unique_key='venue_id' 
) }}

with raw_data as (
    select 
        coalesce(event.value:sport_event.venue.id::string, 'unknown') as venue_id,
        coalesce(event.value:sport_event.id::string, 'unknown') as event_id,
        coalesce(event.value:sport_event.venue.name::string, 'unknown') as venue_name,
        coalesce(event.value:sport_event.venue.capacity::int, 0) as capacity,
        coalesce(event.value:sport_event.venue.city_name::string, 'unknown') as city_name,
        coalesce(event.value:sport_event.venue.country_name::string, 'unknown') as country_name,
        coalesce(event.value:sport_event.venue.map_coordinates::string, 'unknown') as map_coordinates,
        coalesce(event.value:sport_event.venue.country_code::string, 'unknown') as country_code,
        coalesce(event.value:sport_event.venue.timezone::string, 'unknown') as timezone,
        updated_at::timestamp as updated_at
    from {{ source('liverpool', 'sportradar_data') }},
    lateral flatten(input => data:summaries) event
)

select 
    venue_id,
    event_id,
    venue_name,
    capacity,
    city_name,
    country_name,
    map_coordinates,
    country_code,
    timezone,
    updated_at
from raw_data

{% if is_incremental() %}
where updated_at > (select max(updated_at) from {{ this }})
{% endif %}