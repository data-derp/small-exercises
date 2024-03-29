# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Architecture Components

# COMMAND ----------

# DBTITLE 0,Apache Spark
# MAGIC %md
# MAGIC ## Apache Spark
# MAGIC **tl;dr** Spark allows you to run SQL-like or programmatic queries/transformations over very huge dataset leveraging distributed computing. It also supports distributed machine learning**
# MAGIC 
# MAGIC Apache Spark is a <b>unified computing engine</b> and set of <b>libraries</b> for <b>parallel data processing</b> on **computer clusters**. This definition has 3 components that have been broken down below:
# MAGIC 1. **Unified**: Spark can deal with tasks ranging from simple data loading to machine learning and streaming data processing at scale
# MAGIC 2. **Computing engine**: Spark is not a storage system like Hadoop but its features are leveraged for transforming copious amounts of data at high speed. Spark integrates with a lot of persistent storage systems like Apache Hadoop, Amazon S3, Azure Data Lake etc. 
# MAGIC 3. **Libraries**: Spark includes libraries for SQL and structured data, machine learning, stream processing and graph analytics
# MAGIC <br>
# MAGIC 
# MAGIC The Spark framework has 3 components:
# MAGIC 1. **Driver Process**: Runs the main() program and is responsible maintaining information about Spark application, responding to the users query and scheduling and distributing tasks to the executors
# MAGIC 2. **Cluster Manager**: Spark executes the code on multiple nodes. There is a need for a process that manages these resources (health/status etc.). Spark employs a cluster manager to keep track of the resources available.
# MAGIC 3. **Executors** : Are responsible for storage of the data and execution of the code on the distributed data (Reside on the execution nodes, more on this later)

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 _nodes = compute resource utilized to execute queries_

# COMMAND ----------

# DBTITLE 0,Driver, Executor and Cluster Manager
# MAGIC %md
# MAGIC ## Driver, Executor, and Cluster Manager
# MAGIC 
# MAGIC ![Spark Architecture](https://spark.apache.org/docs/latest/img/cluster-overview.png)
# MAGIC 
# MAGIC 
# MAGIC Source: [Spark Architecture](https://spark.apache.org/docs/latest/cluster-overview.html)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Spark Configuration
# MAGIC We can see some of the config of the setup:
# MAGIC - driver host ip (the address where the driver is running): `spark.driver.host`
# MAGIC - driver host port: `spark.driver.port`
# MAGIC - executor's memory: `spark.executor.memory`
# MAGIC 
# MAGIC A full list of the [application configuration can be found here](https://spark.apache.org/docs/latest/configuration.html).

# COMMAND ----------

spark_context = spark.sparkContext
print('Driver host:', spark_context.getConf().get('spark.driver.host'))
print('Driver Port:', spark_context.getConf().get('spark.driver.port'))
print('Spark executor memory:', spark_context.getConf().get('spark.executor.memory'))
print(spark_context.getConf().get('spark.driver.maxResultSize'))

# COMMAND ----------

# MAGIC %md
# MAGIC Some important points
# MAGIC 
# MAGIC - **Each application gets its own executor processes**, which stay up for the duration of the whole application and run tasks in multiple threads. This has the benefit of isolating applications from each other, on both the scheduling side (each driver schedules its own tasks) and executor side (tasks from different applications run in different JVMs). However, it also means that data cannot be shared across different Spark applications (instances of SparkContext) without writing it to an external storage system. In production, you'll need to understand how many Spark applications you are running in order to determine how many machines you'll need and in what configuration.
# MAGIC - **Spark is agnostic to the underlying cluster manager**. As long as it can acquire executor processes, and these communicate with each other, it is relatively easy to run it even on a cluster manager that also supports other applications (e.g. Mesos/YARN/Kubernetes).
# MAGIC - Because the **driver schedules tasks on the cluster, it should be run close to the worker nodes**, preferably on the same local area network. If you’d like to send requests to the cluster remotely, it’s better to open an RPC to the driver and have it submit operations from nearby than to run a driver far away from the worker nodes.