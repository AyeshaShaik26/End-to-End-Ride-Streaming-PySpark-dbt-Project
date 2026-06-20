# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

# MAGIC %md created a dynamic class for transformations

# COMMAND ----------

import os
import sys
sys.path.append(current_dir)

# COMMAND ----------

from custom_utils import transformationss

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

class transformations:
    def dedup(self,df:DataFrame,dedup_cols:List, cdc:str):
        df = df.withColumn("dedup_key", concat(*dedup_cols))
        df = df.withColumn('dedup_counts', row_number()\
                            .over(Window.partitionBy('dedup_key').orderBy(desc(cdc))))
        df =df.filter(col('dedup_counts')==1)
        df = df.drop("dedup_key", 'dedup_counts')
        return df
    
    def process_timestamp(self, df:DataFrame):
        df = df.withColumn("process_timestamp", current_timestamp())
        return df
    def upsert(self,df:DataFrame,key_cols,table,cdc):
        merge_condition = "AND".join([f"src.{i}=trg.{i}"for i in key_cols])
        dlt_obj = DeltaTable.forName(spark, f"pyspark_dbt.silver.{table}")
        dlt_obj.alias('trg').merge(df.alias('src'), merge_condition)\
                            .whenMatchedUpdateAll(condition = f"src.{cdc} >= trg.{cdc}")\
                            .whenNotMatchedInsertAll()\
                            .execute()
        return 1

# COMMAND ----------

# MAGIC %md Customer transformations

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pyspark_dbt.bronze.customers limit 10

# COMMAND ----------

# MAGIC %md ###Customer df creation 

# COMMAND ----------

df_cust = spark.read.table('pyspark_dbt.bronze.customers')

# COMMAND ----------

 df_cust = df_cust.withColumn('domain', split(col("email"), '@')[1])
 df_cust = df_cust.withColumn('phone_number', regexp_replace('phone_number', r'[^0-9]', ''))
 df_cust = df_cust.withColumn('full_name', concat_ws(' ',col("first_name"),col("last_name")))
 display(df_cust)

# COMMAND ----------

cust_obj =  transformations()
df_cust_trns = cust_obj.dedup(df_cust, ['customer_id'], 'last_updated_timestamp')
display(df_cust_trns)

# COMMAND ----------

df_cust = cust_obj.process_timestamp(df_cust_trns)
display(df_cust)

# COMMAND ----------

if not spark.catalog.tableExists("pyspark_dbt.silver.customers"):
    df_cust_trns.write.format("delta")\
                .mode("append")\
                .saveAsTable("pyspark_dbt.silver.customers")
else:
    cust_obj.upsert(df_cust,['customer_id'],'customers','last_updated_timestamp')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from pyspark_dbt.silver.customers

# COMMAND ----------

# MAGIC %md ### Drivers transformations

# COMMAND ----------

df_drive = spark.read.table('pyspark_dbt.bronze.drivers')
display(df_drive)

# COMMAND ----------

 df_drive = df_drive.withColumn('phone_number', regexp_replace('phone_number', r'[^0-9]', ''))
 df_drive = df_drive.withColumn('full_name', concat_ws(' ',col("first_name"),col("last_name")))
 display(df_drive)

# COMMAND ----------

drive_obj =  transformations()
df_drive_trns = drive_obj.dedup(df_drive, ['driver_id'], 'last_updated_timestamp')
display(df_drive_trns)

# COMMAND ----------

df_drive = drive_obj.process_timestamp(df_drive_trns)
display(df_drive)

# COMMAND ----------

if not spark.catalog.tableExists("pyspark_dbt.silver.drivers"):
    df_drive_trns.write.format("delta")\
                .mode("append")\
                .saveAsTable("pyspark_dbt.silver.drivers")
else:
    drive_obj.upsert(df_drive,['driver_id'],'drivers','last_updated_timestamp')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from pyspark_dbt.silver.drivers

# COMMAND ----------

# MAGIC %md ### Locations Transformations

# COMMAND ----------

df_loc = spark.read.table('pyspark_dbt.bronze.locations')
display(df_loc)

# COMMAND ----------

loc_obj =  transformations()
df_loc_trns = loc_obj.dedup(df_loc, ['location_id'], 'last_updated_timestamp')
display(df_loc_trns)

# COMMAND ----------

df_loc = loc_obj.process_timestamp(df_loc_trns)
display(df_loc)

# COMMAND ----------

if not spark.catalog.tableExists("pyspark_dbt.silver.locations"):
    df_loc_trns.write.format("delta")\
                .mode("append")\
                .saveAsTable("pyspark_dbt.silver.locations")
else:
    loc_obj.upsert(df_loc,['location_id'],'locations','last_updated_timestamp')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from pyspark_dbt.silver.locations

# COMMAND ----------

# MAGIC %md ### Payments transformations

# COMMAND ----------

df_pay = spark.read.table('pyspark_dbt.bronze.payments')
display(df_pay)

# COMMAND ----------

df_pay = df_pay.withColumn("online_payment_status",
                when(((col('payment_method') == 'Card') & (col('payment_status')=='Success')), "online-success")
                .when(((col('payment_method') == 'Card') & (col('payment_status')=='Failed')), "online-failed")
                .when(((col('payment_method') == 'Card') & (col('payment_status')=='Pending')), "online-pending")
                .otherwise("offline")
                           )
display(df_pay)

# COMMAND ----------

pay_obj =  transformations()
df_pay_trns = pay_obj.dedup(df_pay, ['payment_id'], 'last_updated_timestamp')
display(df_pay_trns)

# COMMAND ----------

df_pay = pay_obj.process_timestamp(df_pay_trns)
display(df_pay)

# COMMAND ----------

if not spark.catalog.tableExists("pyspark_dbt.silver.payments"):
    df_pay_trns.write.format("delta")\
                .mode("append")\
                .saveAsTable("pyspark_dbt.silver.payments")
else:
    pay_obj.upsert(df_pay,['payment_id'],'payments','last_updated_timestamp')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from pyspark_dbt.silver.payments

# COMMAND ----------

# MAGIC %md ### Vehicle Transformations

# COMMAND ----------

df_vehi = spark.read.table('pyspark_dbt.bronze.vehicles')
df_vehi = df_vehi.withColumn('model', upper(col("model")))
display(df_vehi)

# COMMAND ----------

vehi_obj =  transformations()
df_vehi_trns = vehi_obj.dedup(df_vehi, ['vehicle_id'], 'last_updated_timestamp')
display(df_vehi_trns)

# COMMAND ----------

df_vehi = vehi_obj.process_timestamp(df_vehi_trns)
display(df_vehi)

# COMMAND ----------

if not spark.catalog.tableExists("pyspark_dbt.silver.vehicles"):
    df_vehi_trns.write.format("delta")\
                .mode("append")\
                .saveAsTable("pyspark_dbt.silver.vehicles")
else:
    vehi_obj.upsert(df_vehi,['vehicle_id'],'vehicles','last_updated_timestamp')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from pyspark_dbt.silver.vehicles

# COMMAND ----------

