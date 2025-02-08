# Databricks notebook source
# Define the source path (external volume)
SOURCE_PATH = "<path to files>"

# Define the Bronze table - Raw data ingestion
@dlt.table(comment="Data from files to delta tables in a raw format")
def windmill_bronze():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .load(SOURCE_PATH)
    )


# COMMAND ----------

@dlt.table(comment="Windmill with proper data types and nulls filtered out")

def windmill_silver():
    df = spark.table("windmill_bronze") \
        .selectExpr(
            "cast(`timestamp` as timestamp) as `timestamp`",
            "cast(turbine_id as int) as turbine_id",
            "cast(wind_speed as decimal(10, 1)) as wind_speed",
            "cast(wind_direction as int) as wind_direction",
            "cast(power_output as decimal(10, 1)) as power_output"
        ) \
        .filter("turbine_id is not null and power_output is not null and wind_speed is not null and wind_direction is not null")
    return df

# COMMAND ----------

@dlt.view(comment="Summarized daily averages per turbine")

def windmill_gold():
    df = spark.sql('''
               select 
                turbine_id, 
                min(power_output) as min_power, 
                max(power_output) as max_power, 
                stddev_pop(power_output) stddev_power from windmill_silver
               group by turbine_id
               order by turbine_id
               ''')
    return df

# COMMAND ----------

@dlt.view(comment="power_output is over 2 standard deviotions from daily average")

def windmill_anomaly():
    df = spark.sql('''
            with dayavgs as (
                select 
                    `timestamp`,
                    turbine_id,
                    wind_direction,
                    wind_speed,
                    power_output,
                    avg(power_output) over (partition by  turbine_id, datepart('year', `timestamp`), datepart('month', `timestamp`), datepart('day', `timestamp`) ) as day_avg,
                    stddev(power_output) over (partition by  turbine_id, datepart('year', `timestamp`), datepart('month', `timestamp`), datepart('day', `timestamp`) ) as day_std
                from windmill_silver
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
               ''')
    return df
