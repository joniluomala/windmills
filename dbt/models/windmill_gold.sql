{{
  config(
    materialized = "view",
    sort = 'turbine_id',
  )
}}

select 
  turbine_id, 
  min(power_output) as min_power, 
  max(power_output) as max_power, 
  stddev_pop(power_output) stddev_power 
from {{ ref('windmill_silver') }} 
group by turbine_id