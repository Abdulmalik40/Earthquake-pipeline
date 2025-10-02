from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType
from datetime import date , timedelta


bronze_output = dbutils.jobs.taskValues.get(taskKey="Bronze", key="bronze_output")

start_date = bronze_output.get("start_date", "")
bronze_adls = bronze_output.get("bronze_adls", "")
silver_adls = bronze_output.get("silver_adls", "")

print(f"Start Date: {start_date}, Bronze ADLS: {bronze_adls}")


df = spark.read.option("multiline","true").json(f"{bronze_adls}{start_date}_earthquake_data.json"

df = (
    df
    .select(
        'id',
        col('geometry.coordinates').getItem(0).alias('longitude'),
        col('geometry.coordinates').getItem(1).alias('latitude'),
        col('geometry.coordinates').getItem(2).alias('elevation'),
        col('properties.title').alias('title'),
        col('properties.place').alias('place_description'),
        col('properties.sig').alias('sig'),
        col('properties.mag').alias('mag'),
        col('properties.magType').alias('magType'),
        col('properties.time').alias('time'),
        col('properties.updated').alias('updated')
    )
)
    
df = (
    df
    .withColumn('longitude', when(isnull(col('longitude')), 0).otherwise(col('longitude')))
    .withColumn('latitude', when(isnull(col('latitude')), 0).otherwise(col('latitude')))
    .withColumn('time', when(isnull(col('time')), 0).otherwise(col('time')))
)

df = (
    df
    .withColumn("time",
    (col("time") / 1000).cast(TimestampType())
    )
    .withColumn(
        "update",
        (col("updated") / 1000).cast(TimestampType())
    )
)

silver_output_path = f"{silver_adls}earthquake_events_silver/"
df.write.mode('append').parquet(silver_output_path)
dbutils.jobs.taskValues.set(key = "silver_output", value = silver_output_path)