with match_data as (
    select distinct
        event_id,
        competitor_name,
        season_name,
        start_time,
        competition_name,
        qualifier,
        coalesce(goals, 0) as goals,
        coalesce(competitor_ball_possession, 0) as ball_possession,
        coalesce(competitor_shots_on_target, 0) as shots_on_target,
        coalesce(competitor_passes_successful, 0) as passes_completed,
        coalesce(competitor_chances_created, 0) as chances_created,
        coalesce(competitor_tackles_successful, 0) as tackles_successful,
        coalesce(competitor_interceptions, 0) as interceptions, 
        coalesce(competitor_clearances, 0) as clearances,
        coalesce(competitor_corner_kicks, 0) as corner_kicks,
        coalesce(competitor_shots_saved, 0) as shots_saved,        
        coalesce(competitor_fouls, 0) as fouls_committed
    from {{ ref('int__sport_event_summary') }}
)

select 
    competitor_name,
    season_name,
    competition_name,
    date(start_time) as match_date,
    count(match_date) over (partition by competitor_name, competition_name) as no_of_matches_played,
    goals,
    ball_possession,
    shots_on_target,
    passes_completed,
    chances_created,
    tackles_successful,
    interceptions,
    clearances,
    corner_kicks,
    shots_saved,
    fouls_committed,
    sum(goals) over (partition by competitor_name, competition_name) as total_goals_scored,
    round(coalesce(avg(ball_possession) over (partition by competitor_name, competition_name), 0), 0) as avg_ball_possession_per_competition,
    round(coalesce(avg(shots_on_target) over (partition by competitor_name, competition_name), 0), 0) as avg_shots_on_target_per_competition,
    round(coalesce(avg(passes_completed) over (partition by competitor_name, competition_name), 0), 0) as avg_passes_completed_per_competition,
    round(coalesce(avg(chances_created) over (partition by competitor_name, competition_name), 0), 0) as avg_chances_created,
    round(coalesce(avg(tackles_successful) over (partition by competitor_name, competition_name), 0), 0) as avg_tackles_successful,
    round(coalesce(avg(interceptions) over (partition by competitor_name, competition_name), 0), 0) as avg_interceptions,
    round(coalesce(avg(clearances) over (partition by competitor_name, competition_name), 0), 0) as avg_clearances,
    round(coalesce(avg(corner_kicks) over (partition by competitor_name, competition_name), 0), 0) as avg_corner_kicks,
    round(coalesce(avg(shots_saved) over (partition by competitor_name, competition_name), 0), 0) as avg_shots_saved,
    round(coalesce(avg(fouls_committed) over (partition by competitor_name, competition_name), 0), 0) as avg_fouls_committed_per_competition
from match_data
order by competitor_name, competition_name