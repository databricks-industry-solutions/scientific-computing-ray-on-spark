# Databricks notebook source
# MAGIC %md # Convert Item CSV to Parquet using Spark
# MAGIC
# MAGIC Read in CSV from our local checkout of Github repo with Spark

# COMMAND ----------

import os
input_filepath = "file:"+os.path.join(os.getcwd())+"/sample_item_input.csv"
print("input_filepath:",input_filepath)

items = (spark.read.format("csv")
         .option("header","true")
         .option("inferSchema","true")
         .load(input_filepath)
         )
display(items)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert to Pandas to produce a single Parquet file
# MAGIC
# MAGIC When writing to storage with Pandas, do not include `file:` in directory

# COMMAND ----------

output_filepath = os.path.join(os.getcwd())+"/sample_item_input.parquet"
print("output_filepath:",output_filepath)

items.toPandas().to_parquet(output_filepath)

# COMMAND ----------


