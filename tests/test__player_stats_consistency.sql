with invalid_data as (
    select
        player_name,
        goals_scored,
        total_goals_scored
    from {{ ref('dim__player_each_match_performance') }}
    where 
        total_goals_scored >= goals_scored 
)
select * 
from invalid_data