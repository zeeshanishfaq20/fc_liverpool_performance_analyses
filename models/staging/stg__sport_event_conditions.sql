with raw_data as (
    select
        coalesce(event.value:sport_event.id::string, 'unknown') as event_id,
        coalesce(event.value:sport_event.sport_event_conditions.attendance.count::int, 0) as attendance,
        coalesce(event.value:sport_event.sport_event_conditions.weather.pitch_conditions::string, 'unknown') as pitch_weather_conditions,
        coalesce(event.value:sport_event.sport_event_conditions.weather.overall_conditions::string, 'unknown') as overall_weather_conditions,
        coalesce(event.value:sport_event.sport_event_conditions.ground.neutral::boolean, false) as neutral_ground,
        coalesce(event.value:sport_event.sport_event_conditions.lineups.confirmed::boolean, false) as lineups_confirmed,
        coalesce(referee.value:id::string, 'unknown') as referee_id,
        coalesce(referee.value:name::string, 'unknown') as referee_name,
        coalesce(referee.value:nationality::string, 'unknown') as nationality,
        coalesce(referee.value:country_code::string, 'unknown') as country_code,
        coalesce(referee.value:type::string, 'unknown') as type
    from {{ source('liverpool', 'sportradar_data') }},
    lateral flatten(input => data:summaries) event,
    lateral flatten(input => event.value:sport_event.sport_event_conditions.referees) referee
)

select
    event_id,
    referee_id,
    referee_name,
    nationality,
    country_code,
    type,
    attendance,
    pitch_weather_conditions,
    overall_weather_conditions,
    neutral_ground,
    lineups_confirmed
from raw_data