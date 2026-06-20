# Databricks notebook source
# MAGIC %md reading source data

# COMMAND ----------

# df = spark.read.format('csv')\
#         .option('header', True)\
#         .option('inferSchema', True)\
#         .load("/Volumes/pyspark_dbt/source/source_data/customers/")

# COMMAND ----------

# display(df)

# COMMAND ----------

# customer_schema = df.schema
# customer_schema

# COMMAND ----------

# MAGIC %md ###Spark Streaming with dynamic configuration

# COMMAND ----------

entities = ['customers', 'drivers', 'trips', 'locations', 'payments', 'vehicles']

# COMMAND ----------

for entity in entities:
    #reading all the schemas and assign to entity schema
    df_batch = spark.read.format('csv')\
        .option('header', True)\
        .option('inferSchema', True)\
        .load(f"/Volumes/pyspark_dbt/source/source_data/{entity}/")
    
    entity_schema = df_batch.schema

    #reading each csv file from source as stream
    df = spark.readStream.format('csv')\
            .option('header', True)\
            .schema(entity_schema)\
            .load(f"/Volumes/pyspark_dbt/source/source_data/{entity}/")
    
    #writing stream to the bronze table in delta format
    df.writeStream.format("delta")\
        .outputMode("append")\
        .option("checkpointLocation", f"/Volumes/pyspark_dbt/bronze/checkpoint/{entity}")\
        .trigger(once=True)\
        .toTable(f"pyspark_dbt.bronze.{entity}")


# COMMAND ----------

