# Databricks notebook source
# DBTITLE 0,Components of Spark query execution
# MAGIC %md
# MAGIC # Spark Execution
# MAGIC 
# MAGIC ## Components of Spark Query Execution
# MAGIC 
# MAGIC 1. **Cluster manager**: Manages the lifecycle of the worker processes; Starts worker processes on worker nodes; Typically managed by service provider (e.g. Databricks).
# MAGIC 3. **Cluster driver process**: Serves requests to start cluster worker processes; Tied to the cluster manager.
# MAGIC 4. **Cluster worker process**: Maintains Spark Executor processes and Driver process; Tied to the worker nodes.
# MAGIC 2. **Worker node**: Hosts the worker processes; In cluster mode, also runs driver process; Compute resources (VM, container, serverless function).
# MAGIC 6. **Client node**: Client mode only; Hosts the application code.
# MAGIC 5. **Driver process**: Executes code and exchanges data between executor processes; Either resides on a worker node (cluster mode) or a client node (client mode).
# MAGIC 5. **Executor process**: Executes Spark queries.
# MAGIC 6. **Client application**: Your spark code! 🤗

# COMMAND ----------

# DBTITLE 0,Spark model sans Spark application 
# MAGIC %md
# MAGIC ## Spark Architecture (without a running Spark application)
# MAGIC ![Spark framework with no Spark Application running](https://raw.githubusercontent.com/Realsid/databricks-spark-certification/master/assets/mode%20diagrams-no%20spark%20application.png)
# MAGIC 
# MAGIC Source: [Cluster Mode](https://spark.apache.org/docs/3.0.0-preview/cluster-overview.html)

# COMMAND ----------

# DBTITLE 0,Execution modes is Spark
# MAGIC %md
# MAGIC 
# MAGIC ## Execution Modes in Spark
# MAGIC 
# MAGIC Spark offers choice of execution modes, which differ in where different process are physically located
# MAGIC 1. Cluster
# MAGIC 2. Client
# MAGIC 3. Local

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cluster Mode
# MAGIC 
# MAGIC ![Cluster mode](https://raw.githubusercontent.com/Realsid/databricks-spark-certification/master/assets/mode%20diagrams-cluster%20mode.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Client Mode
# MAGIC 
# MAGIC ![Client mode](https://raw.githubusercontent.com/Realsid/databricks-spark-certification/master/assets/mode%20diagrams-client%20mode.png)
# MAGIC [3] Client mode

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution of a Spark Application
# MAGIC 1. Submit the code to be executed using the `spark-submit` command
# MAGIC 2. **Cluster manager allocates resources** for driver process
# MAGIC 3. **Spark session initialises spark cluster** (driver + executors) by **communicating with cluster manager** (number of executors are a configuration set by the user)
# MAGIC 4. **Cluster manager initiates executors** and send their information (location etc.) to the spark cluster
# MAGIC 5. **Driver now communicates with the workers** to perform the task. This involves **moving data around (shuffling) and executing code**. Each worker responds with the status of those task

# COMMAND ----------

# DBTITLE 0,Spark application execution at code level
# MAGIC %md
# MAGIC ## Execution of the code of a Spark Application
# MAGIC The Spark application execution has the following components
# MAGIC 1. **Sparksession**: SparkSession provides the entry point for a spark application to interact with the underlying compute.
# MAGIC 2. **Spark Job**: A job corresponds to one "Action" that triggers execution on a set of dataFrame/table projection/transformation
# MAGIC 3. **Stages**: Stages are a logical grouping of different tasks that have to be performed on a dataset
# MAGIC 4. **Tasks**: Task is an atomic unit of work performed on a block of data

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 _Spark works according to the **lazy execution** paradigm i.e. operations can be chained and will not be performed until the result is required_

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 _Spark API provides a handful of "Action" methods (take, collect, show) to retrieve the results of your operations. More on this later !_

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo
# MAGIC In this demo, we'll run some queries on some data and see how it manifests itself in Spark jobs/tasks.

# COMMAND ----------

# MAGIC %run ../init

# COMMAND ----------

# MAGIC %fs 
# MAGIC head /databricks-datasets/iot/iot_devices.json

# COMMAND ----------

file_path = '/databricks-datasets/iot/iot_devices.json'

# read the data into dataframe
df = spark.read.json(file_path)

# select a column
projection_1 = df.selectExpr('battery_level as `Battery Level`')

# repartition 
repart = projection_1.repartition(20)

# find max
max_ = repart.selectExpr('max(`Battery Level`)')
max_.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Expanding the jobs, you should see the following:
# MAGIC 
# MAGIC 1. **Job 1**: Read data
# MAGIC     - Stage 1, 8 tasks: Read the data, by default the dataframe is read into 8 partitions
# MAGIC 2. **Job 2**: Select a column from the data frame, repartition the data into 20 partitions and get maximum of the column
# MAGIC     - Stage 1, 8 tasks: Select the column
# MAGIC     - Stage 2, 20 tasks: Repartition the data
# MAGIC     - Stage 3, 1 task: Get maximum value