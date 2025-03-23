with player_stats as (
    select distinct
        event_id,
        player_name,
        competitor_name,
        competition_name,
        season_name,
        start_time,
        coalesce(assists, 0) as assists,
        coalesce(passes_successful, 0) as passes_successful,
        coalesce(shots_on_target, 0) as shots_on_target,
        coalesce(minutes_played, 0) as minutes_played,        
        coalesce(goals_scored, 0) as goals_scored,
        coalesce(offsides, 0) as offsides,
        coalesce(own_goals, 0) as own_goals,
        coalesce(red_cards, 0) as red_cards,
        coalesce(shots_blocked, 0) as shots_blocked,
        coalesce(yellow_cards, 0) as yellow_cards,
        coalesce(yellow_red_cards, 0) as yellow_red_cards
    from {{ ref('int__sport_event_summary') }}
)

select distinct
    event_id,
    player_name,
    competitor_name,
    competition_name,
    season_name,
    date(start_time) as match_date,
    count(match_date) over (partition by player_name, competitor_name, competition_name) as no_of_matches_played,
    assists,
    passes_successful,
    shots_on_target,
    minutes_played,        
    goals_scored,
    offsides,
    own_goals,
    red_cards,
    shots_blocked,
    yellow_cards,
    sum(assists) over (partition by player_name, competitor_name, competition_name) as total_assists,
    sum(passes_successful) over (partition by player_name, competitor_name, competition_name) as total_passes_successful,
    sum(shots_on_target) over (partition by player_name, competitor_name, competition_name) as total_shots_on_target,
    sum(minutes_played) over (partition by player_name, competitor_name, competition_name) as total_minutes_played,
    sum(goals_scored) over (partition by player_name, competitor_name, competition_name) as total_goals_scored,
    sum(offsides) over (partition by player_name, competitor_name, competition_name) as total_offsides,
    sum(own_goals) over (partition by player_name, competitor_name, competition_name) as total_own_goals,
    sum(red_cards) over (partition by player_name, competitor_name, competition_name) as total_red_cards,
    sum(shots_blocked) over (partition by player_name, competitor_name, competition_name) as total_shots_blocked,
    sum(yellow_cards) over (partition by player_name, competitor_name, competition_name) as total_yellow_cards
from player_stats
order by competitor_name, competition_name