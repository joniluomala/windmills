select 
  cast(`timestamp` as timestamp) as `timestamp`, 
  cast(turbine_id as int) as turbine_id, 
  cast(wind_speed as decimal(10, 1)) as wind_speed, 
  cast(wind_direction as int) as wind_direction, 
  cast(power_output as decimal(10, 1)) as power_output
from {{ ref('windmill_bronze') }}  