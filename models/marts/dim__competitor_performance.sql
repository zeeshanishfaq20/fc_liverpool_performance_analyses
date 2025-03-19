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
        coalesce(competitor_was_fouled, 0) as fouls_committed
    from {{ ref('stg__sport_event_summary') }}
)

select
    distinct
    competitor_name,
    season_name,
    competition_name,
    date(start_time) as match_date,
    goals,
    ball_possession,
    shots_on_target,
    passes_completed,
    fouls_committed,
    sum(goals) over (partition by competitor_name, competition_name) as total_goals_per_competition,
    round(coalesce(avg(ball_possession) over (partition by competitor_name, competition_name), 0), 0) as avg_ball_possession_per_competition,
    round(coalesce(avg(shots_on_target) over (partition by competitor_name, competition_name), 0), 0) as avg_shots_on_target_per_competition,
    round(coalesce(avg(passes_completed) over (partition by competitor_name, competition_name), 0), 0) as avg_passes_completed_per_competition,
    round(coalesce(avg(fouls_committed) over (partition by competitor_name, competition_name), 0), 0) as avg_fouls_committed_per_competition
from match_data
order by competition_name, competitor_name, match_date