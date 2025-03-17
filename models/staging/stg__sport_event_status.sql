with raw_data as (
    select 
        coalesce(event.value:sport_event.id::string, 'unknown') as event_id,
        coalesce(event.value:sport_event_status.status::string, 'unknown') as status,
        coalesce(event.value:sport_event_status.match_status::string, 'unknown') as match_status,
        coalesce(event.value:sport_event_status.home_score::int, 0) as home_score,
        coalesce(event.value:sport_event_status.away_score::int, 0) as away_score,
        coalesce(event.value:sport_event_status.home_normaltime_score::int, 0) as home_normaltime_score,
        coalesce(event.value:sport_event_status.away_normaltime_score::int, 0) as away_normaltime_score,
        coalesce(event.value:sport_event_status.home_overtime_score::int, 0) as home_overtime_score,
        coalesce(event.value:sport_event_status.away_overtime_score::int, 0) as away_overtime_score,
        coalesce(event.value:sport_event_status.winner_id::string, 'unknown') as winner_id,
        coalesce(event.value:sport_event_status.aggregate_home_score::int, 0) as aggregate_home_score,
        coalesce(event.value:sport_event_status.aggregate_away_score::int, 0) as aggregate_away_score,
        coalesce(event.value:sport_event_status.aggregate_winner_id::string, 'unknown') as aggregate_winner_id
    from {{ source('liverpool', 'sportradar_data') }},
    lateral flatten(input => data:summaries) event
)

select 
    event_id,
    status,
    match_status,
    home_score,
    away_score,
    home_normaltime_score,
    away_normaltime_score,
    home_overtime_score,
    away_overtime_score,
    winner_id,
    aggregate_home_score,
    aggregate_away_score,
    aggregate_winner_id
from raw_data