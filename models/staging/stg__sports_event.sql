{{ config(
    materialized='incremental',
    unique_key='event_id' 
) }}

with raw_data as (
    select 
        coalesce(event.value:sport_event.id::string, 'unknown') as event_id,
        event.value:sport_event.start_time::timestamp_ntz as start_time, 
        coalesce(event.value:sport_event.start_time_confirmed::boolean, false) as start_time_confirmed,
        coalesce(event.value:sport_event.sport_event_context.sport.id::string, 'unknown') as sport_id,
        coalesce(event.value:sport_event.sport_event_context.sport.name::string, 'unknown') as sport_name,
        coalesce(event.value:sport_event.sport_event_context.category.id::string, 'unknown') as category_id,
        coalesce(event.value:sport_event.sport_event_context.category.name::string, 'unknown') as category_name,
        coalesce(event.value:sport_event.sport_event_context.competition.id::string, 'unknown') as competition_id,
        coalesce(event.value:sport_event.sport_event_context.competition.name::string, 'unknown') as competition_name,
        coalesce(event.value:sport_event.sport_event_context.competition.gender::string, 'unknown') as gender,
        coalesce(event.value:sport_event.sport_event_context.season.id::string, 'unknown') as season_id,
        coalesce(event.value:sport_event.sport_event_context.season.name::string, 'unknown') as season_name,
        event.value:sport_event.sport_event_context.season.start_date::date as season_start_date,
        event.value:sport_event.sport_event_context.season.end_date::date as season_end_date,
        coalesce(event.value:sport_event.sport_event_context.season.year::string, 'unknown') as year,
        coalesce(event.value:sport_event.sport_event_context.stage.order::int, 0) as stage_order,
        coalesce(event.value:sport_event.sport_event_context.stage.type::string, 'unknown') as stage_type,
        coalesce(event.value:sport_event.sport_event_context.stage.phase::string, 'unknown') as phase,
        event.value:sport_event.sport_event_context.stage.start_date::date as stage_start_date,
        event.value:sport_event.sport_event_context.stage.end_date::date as stage_end_date,
        coalesce(event.value:sport_event.sport_event_context.stage.year::string, 'unknown') as stage_year,
        coalesce(event.value:sport_event.sport_event_context.round.name::string, 'unknown') as round_name,
        coalesce(event.value:sport_event.sport_event_context.round.cup_round_sport_event_number::int, 0) as cup_round_sport_event_number,
        coalesce(event.value:sport_event.sport_event_context.round.cup_round_number_of_sport_events::int, 0) as cup_round_number_of_sport_events,
        coalesce(event.value:sport_event.sport_event_context.round.cup_round_id::string, 'unknown') as cup_round_id,
        coalesce(event.value:sport_event.sport_event_context.groups[0].id::string, 'unknown') as group_id,
        coalesce(event.value:sport_event.sport_event_context.groups[0].name::string, 'unknown') as group_name,
        updated_at::timestamp as updated_at
    from {{ source('liverpool', 'sportradar_data') }},
    lateral flatten(input => data:summaries) event
)

select 
    event_id,
    start_time, 
    start_time_confirmed,
    sport_id,
    sport_name,
    category_id,
    category_name,
    competition_id,
    competition_name,
    gender,
    season_id,
    season_name,
    season_start_date,
    season_end_date,
    year,
    stage_order,
    stage_type,
    phase,
    stage_start_date,
    stage_end_date,
    stage_year,
    round_name,
    cup_round_sport_event_number,
    cup_round_number_of_sport_events,
    cup_round_id,
    group_id,
    group_name,
    updated_at
from raw_data

{% if is_incremental() %}
where updated_at > (select max(updated_at) from {{ this }})
{% endif %}