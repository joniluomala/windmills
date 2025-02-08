{{
  config(
    materialized = "view",
    sort = 'turbine_id',
  )
}}

with dayavgs as (
  select 
    `timestamp`,
    turbine_id,
    wind_direction,
    wind_speed,
    power_output,
    avg(power_output) over (partition by  turbine_id, datepart('year', `timestamp`), datepart('month', `timestamp`), datepart('day', `timestamp`) ) as day_avg,
    stddev(power_output) over (partition by  turbine_id, datepart('year', `timestamp`), datepart('month', `timestamp`), datepart('day', `timestamp`) ) as day_std
from {{ ref('windmill_silver') }} 
)
select  
  dayavgs.`timestamp`,
  dayavgs.turbine_id,
  dayavgs.wind_direction,
  dayavgs.wind_speed,
  dayavgs.power_output,
  dayavgs.day_avg,
  dayavgs.day_std
 from dayavgs 
 where power_output > day_avg + 2 * day_std or power_output < day_avg - 2 * day_std