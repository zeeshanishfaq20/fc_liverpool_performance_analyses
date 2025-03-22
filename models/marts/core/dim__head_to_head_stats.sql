with head_to_head as (
    select distinct
        event_id,
        competitor_id,
        competitor_name,
        competition_name,
        date(start_time) as match_date,
        goals,
        winner_id,
        match_tie,
        match_status
    from {{ ref('int__sport_event_summary') }}
),

aggregated_head_to_head as (
    select
        event_id,
        competitor_id,
        competitor_name,
        competition_name,
        match_date,
        goals,
        winner_id,
        match_tie,
        match_status,
        row_number() over (partition by event_id order by competitor_name) as competitor_rank,
    from head_to_head
    where match_date <= current_date
),

filtered_head_to_head as (
    select distinct
        event_id,
        competition_name,
        match_date,
        max(case when competitor_rank = 1 then competitor_id end) as competitor_1_id,
        max(case when competitor_rank = 2 then competitor_id end) as competitor_2_id,
        max(case when competitor_rank = 1 then competitor_name end) as competitor_1_name,
        max(case when competitor_rank = 2 then competitor_name end) as competitor_2_name,
        max(case when competitor_rank = 1 then goals end) as competitor_1_goals,
        max(case when competitor_rank = 2 then goals end) as competitor_2_goals,
        winner_id,
        match_tie,
        match_status
from aggregated_head_to_head
group by event_id, match_date, competition_name, winner_id, match_tie, match_status
order by event_id
)

select
        event_id,
        competition_name,
        match_date,
        competitor_1_name,
        competitor_2_name,
        competitor_1_goals,
        competitor_2_goals,
        case
            when winner_id = competitor_1_id then competitor_1_name
            when winner_id = competitor_2_id then competitor_2_name
            else 'Draw'
        end as winner_name,   
        match_tie,
        match_status
from filtered_head_to_head