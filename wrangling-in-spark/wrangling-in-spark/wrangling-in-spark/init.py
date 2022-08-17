# Databricks notebook source
'''
Clear out existing working directory
'''
current_user=dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().split("@")[0]
working_directory=f"/FileStore/{current_user}/wrangling-with-spark"
dbutils.fs.rm(working_directory, True)
dbutils.fs.mkdirs(working_directory)

# COMMAND ----------

# Turn off AQE
# AQE was introduced in Spark 3.0 which optimises your queries
# We turn it off here to demonstrate natural behaviour
# More info: https://docs.databricks.com/spark/latest/spark-sql/aqe.html#enable-and-disable-adaptive-query-execution

print(spark.conf.get('spark.databricks.optimizer.adaptive.enabled'))
spark.conf.set('spark.databricks.optimizer.adaptive.enabled','false')