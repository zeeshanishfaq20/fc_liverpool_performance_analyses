with player_stats as (
    select distinct
        event_id,
        competitor_id,
        player_name,
        assists,
        passes_successful,
        shots_on_target,
        minutes_played
    from {{ ref('fct__sport_event_summary') }}       
)

select
    player_name,
    competitor_id,
    count(event_id) as matches_played,
    sum(assists) as total_assists,
    sum(passes_successful) as total_passes_successful,
    sum(shots_on_target) as total_shots_on_target,
    sum(minutes_played) as total_minutes_played
from player_stats
group by player_name, competitor_id