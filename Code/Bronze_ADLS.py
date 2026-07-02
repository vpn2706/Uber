# Databricks notebook source
# MAGIC %md 
# MAGIC ### Using Pandas to load the dataframe for reading the Mapping files 

# COMMAND ----------

import pandas as pd 
from pyspark.sql.functions import *
from pyspark.sql.types import * 

# COMMAND ----------


files = [
{'file':'map_cities' },
{"file":"map_cancellation_reasons"},
{"file":"bulk_rides"},
{"file":"map_payment_methods"},
{"file":"map_ride_statuses"},
{"file":"map_vehicle_makes"},
{"file":"map_vehicle_types"}
]

load_ts = "2026-07-01 22:00:00" 

for file in files:
    url = f'https://dluber2706.blob.core.windows.net/raw/ingestion/{file['file']}.json?sp=r&st=2026-07-01T16:33:29Z&se=2026-07-02T00:48:29Z&spr=https&sv=2026-02-06&sr=c&sig=l8ixIM8XUxTqdADdkstq5hQtWir6uSHHgjwqdt9x6x8%3D'
    df = pd.read_json(url)
    df_pyspark = spark.createDataFrame(df)

    #Adding current_timestamp manually for SCD 
    if file['file'] == 'map_cities':
        df_pyspark = df_pyspark.withColumn('updated_at', lit(load_ts).cast(TimestampType()))


    # Writing the data to the bronze layer
    if file['file']=='map_cities':
        df_pyspark.write.format('delta')\
            .mode('overwrite')\
            .option('overwriteSchema', 'true')\
            .saveAsTable(f'uber.bronze.{file['file']}')
    else:
        df_pyspark.write.format('delta')\
                        .mode('overwrite')\
                        .saveAsTable(f'uber.bronze.{file['file']}')


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM uber.bronze.rides_raw

# COMMAND ----------

spark.table('uber.bronze.map_cities').printSchema()

# COMMAND ----------

spark.table("uber.bronze.map_cities").show()

# COMMAND ----------

