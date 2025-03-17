with raw_data as (
    select 
        coalesce(competitor.value:id::string, 'unknown') as competitor_id,
        coalesce(event.value:sport_event.id::string, 'unknown') as event_id,
        coalesce(competitor.value:name::string, 'unknown') as competitor_name,
        coalesce(competitor.value:country::string, 'unknown') as country,
        coalesce(competitor.value:country_code::string, 'unknown') as country_code,
        coalesce(competitor.value:abbreviation::string, 'unknown') as abbreviation,
        coalesce(competitor.value:qualifier::string, 'unknown') as qualifier,
        coalesce(competitor.value:gender::string , 'unknown') as gender
    from {{ source('liverpool', 'sportradar_data') }},
    lateral flatten(input => data:summaries) event,
    lateral flatten(input => event.value:sport_event.competitors) competitor
)

select 
    competitor_id,
    event_id,
    competitor_name,
    country,
    country_code,
    abbreviation,
    qualifier,
    gender
from raw_data