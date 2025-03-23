with top_players as (
    select
        event_id,
        player_name,
        competitor_name,
        competition_name,
        season_name,
        match_date,
        no_of_matches_played,
        goals_scored,
        assists,
        passes_successful,
        shots_on_target,
        minutes_played,
        offsides,
        own_goals,
        red_cards,
        shots_blocked,
        yellow_cards,
        total_goals_scored,
        total_assists,
        total_passes_successful,
        total_shots_on_target,
        total_minutes_played,
        total_offsides,
        total_own_goals,
        total_red_cards,
        total_shots_blocked,
        total_yellow_cards,
        row_number() over (
            partition by competitor_name, competition_name
            order by 
            total_goals_scored desc,
            total_shots_on_target desc, 
            total_assists desc,
            total_own_goals asc,
            total_red_cards asc,
            total_passes_successful desc,
            total_shots_blocked desc,
            total_yellow_cards asc,
            total_offsides asc
            ) as player_rank
from {{ ref('dim__player_each_match_performance')}}
)

select 
    event_id,
    player_name,
    competitor_name,
    competition_name,
    season_name,
    match_date,
    no_of_matches_played,
    total_minutes_played,
    total_goals_scored,
    total_assists,
    total_shots_on_target,
    total_own_goals,
    total_red_cards,
    total_passes_successful,
    total_offsides,
    total_shots_blocked,
    total_yellow_cards,
from top_players
WHERE player_rank <= 3
ORDER BY competitor_name, competition_name, player_rank