# Databricks notebook source
# MAGIC %md
# MAGIC # DataFrames
# MAGIC A Spark DataFrame is a distributed collection of data organized into named columns. From there programmatic queries can be made against this data.
# MAGIC 
# MAGIC * [Read about DataFrames](https://spark.apache.org/docs/latest/api/python//reference/pyspark.sql/api/pyspark.sql.DataFrame.html#pyspark.sql.DataFrame)
# MAGIC 
# MAGIC In this notebook, we'll demonstrate how to:
# MAGIC 1. Create a Spark DataFrame using a list
# MAGIC 2. Create a Spark DataFrame using a set
# MAGIC 3. Create a Spark DataFrame using a Pandas DataFrame
# MAGIC 4. Create a DataFrame for a range of numbers
# MAGIC 6. Register a User Defined Function (UDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up the notebook

# COMMAND ----------

# MAGIC %run ../init

# COMMAND ----------

# MAGIC %run ../init_data

# COMMAND ----------

dbutils.fs.ls(working_directory)

# COMMAND ----------

dbutils.fs.ls("./databricks-datasets/flights")

# COMMAND ----------

# MAGIC %md
# MAGIC `SparkSession` is the driver program (encapsulates SparkContext) that interacts with the underlying cluster.
# MAGIC 
# MAGIC [Read more about SparkSession](https://www.databricks.com/blog/2016/08/15/how-to-use-sparksession-in-apache-spark-2-0.html)

# COMMAND ----------

# In Databricks, SparkSession is available through a variable
spark

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Create a Spark DataFrame using a list

# COMMAND ----------

int_list = [[i] for i in range(100)]
df = spark.createDataFrame(int_list, ['Numbers'])
display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Create a Spark DataFrame using a set

# COMMAND ----------

a_set = (('Manu','100'),('Aditya','90'))
df = spark.createDataFrame(a_set, ['Name', 'EnergyMeter'])
display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Create a Spark DataFrame using a Pandas DataFrame

# COMMAND ----------

pandas_dataframe = pd.DataFrame({'first':range(200), 'second':range(300,500)})
df = spark.createDataFrame(pandas_dataframe)
display(df)

print(type(pandas_dataframe))
print(type(df))

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Create a Spark DataFrame from a range of numbers
# MAGIC We'll be using the [range](https://spark.apache.org/docs/latest/api/python//reference/pyspark.sql/api/pyspark.sql.SparkSession.range.html) method to create a Spark DataFrame.

# COMMAND ----------

spark.range(100).collect()