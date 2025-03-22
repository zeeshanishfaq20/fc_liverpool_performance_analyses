with schedule as (
    select 
        event_id,
        competitor_name,
        competition_name,
        date(start_time) as match_date,
        venue_name as venue,
        match_status
    from {{ ref('int__sport_event_summary') }}
),

ranked_competitors as (
    select
        event_id,
        competitor_name,
        competition_name,
        match_date,
        venue,
        row_number() over (partition by event_id order by competitor_name) as competitor_rank,
        match_status
    from schedule
    where match_date >= current_date
)

select
    event_id,
    max(case when competitor_rank = 1 then competitor_name end) as competitor_1,
    max(case when competitor_rank = 2 then competitor_name end) as competitor_2,
    match_date,
    competition_name,
    venue,
    match_status
from ranked_competitors
group by event_id, match_date, competition_name, venue, match_status
order by event_id