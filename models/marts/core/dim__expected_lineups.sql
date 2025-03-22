with lineups as (
    select distinct
        event_id,
        competition_name,
        competitor_name,
        start_time,
        player_name,
        starter
    from {{ ref('int__sport_event_summary') }}    
),

aggregation_of_lineup as (
    select distinct
        player_name,
        competitor_name,
        competition_name,
        date(start_time) as match_date,
        count(match_date) over (partition by player_name, competition_name, competitor_name) as no_of_matches_played,
        starter,
    from lineups
    order by player_name, competitor_name, competition_name
),

matches_played as (
    select
        competitor_name,
        competition_name,
        player_name,
        no_of_matches_played,
        starter,
        count(starter) over (partition by player_name, competitor_name, competition_name) as no_of_matches_as_starter,
        row_number() over (partition by competitor_name, competition_name order by match_date desc) as row_num
    from aggregation_of_lineup
    where starter = true
    order by competitor_name, competition_name, player_name, no_of_matches_as_starter desc
)

select distinct
    competitor_name,
    competition_name,
    player_name,
    no_of_matches_played,
    no_of_matches_as_starter
from matches_played
where row_num <= 11
order by competitor_name, competition_name, no_of_matches_as_starter desc, player_name 