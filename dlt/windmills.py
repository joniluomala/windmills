# Databricks notebook source
SOURCE_PATH = "<path to files>"

@dlt.table(
    comment="Data from files to delta tables in a raw format",
    table_properties={"quality": "bronze"}
    )
def windmill_bronze():
    return (
        spark.readStream.format("cloudfiles")
        .option("cloudFiles.format", "csv") 
        .option("header", "true")
        .option("inferSchema", "true")
        .load(SOURCE_PATH)
    )


# COMMAND ----------

rules = {
    "valid_timestamp": "wind_direction < 360",
    "valid_turbine_id": "turbine_id  is not null",
    "valid_power_output": "power_output is not null",
    "valid_wind_speed": "wind_speed is not null",
    "valid_wind_direction": "wind_direction is not null"
}

@dlt.table(
    comment="Windmill with proper data types and nulls filtered out",
    table_properties={"quality": "silver"}
    )
@dlt.expect_all_or_drop(rules)

def windmill_silver():
    df = spark.table("windmill_bronze") \
        .selectExpr(
            "cast(`timestamp` as timestamp) as `timestamp`",
            "cast(turbine_id as int) as turbine_id",
            "cast(wind_speed as decimal(10, 1)) as wind_speed",
            "cast(wind_direction as int) as wind_direction",
            "cast(power_output as decimal(10, 1)) as power_output"
        ) \
        .dropDuplicates(["timestamp", "turbine_id"])
    return df

# COMMAND ----------

quarantine_rules = {}
quarantine_rules["invalid_record"] = f"NOT ({' AND '.join(rules.values())})"

@dlt.table
@dlt.expect_all_or_drop(quarantine_rules)
def windmill_quarantine():
    return spark.table("windmill_bronze")

# COMMAND ----------

@dlt.table(comment="Summarized daily averages per turbine")

@dlt.expect("valid_turbine_id", "turbine_id is not null")

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

@dlt.table(comment="power_output is over 2 standard deviotions from daily average")

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
