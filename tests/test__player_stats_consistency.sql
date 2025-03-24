with invalid_data as (
    select 
        assists,
        passes_successful,
        shots_on_target,
        minutes_played,        
        goals_scored,
        offsides,
        own_goals,
        red_cards,
        shots_blocked,
        yellow_cards
    from {{ ref('dim__player_each_match_performance') }}
    where 
        total_assists >= assists  and 
        total_passes_successful >= passes_successful  and
        total_shots_on_target >= shots_on_target  and
        total_minutes_played >= minutes_played  and
        total_goals_scored >= goals_scored  and
        total_offsides >= offsides  and
        total_own_goals >= own_goals  and
        total_red_cards >= red_cards  and
        total_shots_blocked >= shots_blocked  and
        total_yellow_cards >= yellow_cards 
)
select * 
from invalid_data