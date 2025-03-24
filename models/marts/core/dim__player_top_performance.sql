with top_players as (
    select distinct
        player_name,
        competitor_name,
        competition_name,
        season_name,
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
        total_yellow_cards
    from {{ ref('dim__player_each_match_performance')}}
),

ranked_players as (
    select
        player_name,
        competitor_name,
        competition_name,
        season_name,
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
        dense_rank() over (            
            partition by competitor_name, competition_name
            order by 
                case when total_goals_scored > 0 then total_goals_scored else null end desc nulls last,
                case when total_shots_on_target > 0 then total_shots_on_target else null end desc nulls last, 
                case when total_assists > 0 then total_assists else null end desc nulls last,
                case when total_own_goals > 0 then null else 0 end asc nulls last,
                case when total_red_cards > 0 then null else 0 end asc nulls last,
                case when total_yellow_cards > 0 then null else 0 end asc nulls last,
                case when total_offsides > 0 then null else 0 end asc nulls last,          
                total_passes_successful desc,
                total_minutes_played desc,
                total_shots_blocked desc
                ) as player_rank
        from top_players
)

select distinct
    player_name,
    competitor_name,
    competition_name,
    season_name,
    no_of_matches_played,
    total_goals_scored,
    total_assists,
    total_shots_on_target,
    total_own_goals,
    total_red_cards,
    total_passes_successful,
    total_offsides,
    total_shots_blocked,
    total_yellow_cards,
    total_minutes_played,
    player_rank
from ranked_players
WHERE player_rank <= 3
ORDER BY competitor_name, competition_name, player_rank