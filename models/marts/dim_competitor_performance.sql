with match_data as (
    select 
        event_id,
        season_id,
        competition_id,
        start_time,
        team1_name,
        team2_name,
        case 
        when team1_name = 'FC Liverpool' then team1_id
        when team2_name = 'FC Liverpool' then team2_id
        end as competitor_id,
        case
        when team1_name = 'FC Liverpool' then team1_name
        when team2_name = 'FC Liverpool' then team2_name
        end as competitor_name,
        case
        when team1_name = 'FC Liverpool' then home_score
        when team2_name = 'FC Liverpool' then away_score
        end as competitor_score,
        team_ball_possession as possession,
        team_shots_on_target as shots_on_target,
        team_passes_successful as passes_completed,
        team_was_fouled as fouls_committed
    from {{ ref('stg__sport_event_summary') }}
)

select 
    event_id,
    season_id,
    competition_id,
    competitor_id,
    date(start_time) as match_date,
    sum(competitor_score) as total_goals,
    avg(possession) as avg_possession,
    avg(shots_on_target) as avg_shots_on_target,
    avg(passes_completed) as avg_passes_completed,
    avg(fouls_committed) as avg_fouls_committed
from match_data
where competitor_name = 'Liverpool FC' and start_time <= current_date()