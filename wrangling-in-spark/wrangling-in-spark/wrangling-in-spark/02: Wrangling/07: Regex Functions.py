# Databricks notebook source
# MAGIC %md
# MAGIC ## Regular Expressions

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

covid_file_path = "/databricks-datasets/COVID/coronavirusdataset/PatientInfo.csv"
covid_df  = spark.read.format("csv")\
        .option("header",True)\
        .option("inferSchema", True)\
        .load(covid_file_path)

display(covid_df)
covid_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## regex_replace

# COMMAND ----------

covid_df.select(f.regexp_replace(f.col("infection_case"), "patient", "PATIENT"),f.col("infection_case")).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## regexp_extract

# COMMAND ----------

covid_df.select(f.regexp_extract(f.col("infection_case"), "patient" ,0).alias("extracted"),f.col("infection_case")).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## instr

# COMMAND ----------

contains_church = f.instr(f.col("infection_case"), "Church") >= 1
contains_hospital = f.instr(f.col("infection_case"), "Hospital") >= 1
covid_df.withColumn("infection_case_church_hospital", contains_church | contains_hospital)\
.select("infection_case", "infection_case_church_hospital").collect()