with invalid_events as (
    select 
        event_id,
        competitor_1,
        competitor_2,
        match_date,
        competition_name,
        venue,
        match_status
    from {{ ref('dim__upcoming_matches') }}
    where match_date >= current_date
)
select * from invalid_events