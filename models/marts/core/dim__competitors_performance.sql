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
    order by competitor_name, competition_name
)

select
    competitor_name,
    competition_name,
    {{ replace_zero_with_mean('ball_possession',  'match_data') }} as avg_ball_possession,
    {{ replace_zero_with_mean('shots_on_target', 'match_data' ) }} as avg_shots_on_target,
    {{ replace_zero_with_mean('passes_completed', 'match_data') }} as avg_passes_completed,
    {{ replace_zero_with_mean('chances_created', 'match_data') }} as avg_chances_created,
    {{ replace_zero_with_mean('tackles_successful', 'match_data') }} as avg_tackles_successful,
    {{ replace_zero_with_mean('interceptions', 'match_data') }} as avg_interceptions, 
    {{ replace_zero_with_mean('clearances', 'match_data') }} as avg_clearances,
    {{ replace_zero_with_mean('corner_kicks', 'match_data') }} as avg_corner_kicks,
    {{ replace_zero_with_mean('shots_saved', 'match_data') }} as avg_shots_saved,        
    {{ replace_zero_with_mean('fouls_committed', 'match_data') }} as avg_fouls_committed
from match_data
group by competitor_id, competition_id