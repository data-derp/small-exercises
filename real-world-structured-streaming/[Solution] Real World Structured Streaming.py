# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC # Structured Streaming in the Real World
# MAGIC 
# MAGIC The goal of this exercise is to understand structured streaming with respect to a real world problem
# MAGIC 
# MAGIC ## Topics
# MAGIC * Recap on Structured Streaming
# MAGIC * Stateful Operations
# MAGIC * Micro-Batch vs Continuous Processing 

# COMMAND ----------

# MAGIC %md
# MAGIC <!-- You can run this notebook in a Databricks environment. Specifically, this notebook has been designed to run in [Databricks Community Edition](http://community.cloud.databricks.com/) as well. -->
# MAGIC To run this notebook, you have to [create a cluster](https://docs.databricks.com/clusters/create.html) with version **Databricks Runtime 7.4 or later** and [attach this notebook](https://docs.databricks.com/notebooks/notebooks-manage.html#attach-a-notebook-to-a-cluster) to that cluster. This happens automatically when running code.<br/>

# COMMAND ----------

# MAGIC %md ## Setup
# MAGIC 
# MAGIC **Optimization Rule of Thumb:**
# MAGIC When you're running a streaming workload with Spark (e.g. analysing Uber events) and your grouping keys are isolated (events from individual remote devices, e.g. Uber users' phones), then it is often recommended to set `spark.sql.shuffle.partitions` to **approximately** the number of cores on your workers. <br> Excessive shuffling can [often become a hidden bottleneck, causing you to scale your cluster unnecessarily](https://medium.com/@manuelmourato25/how-spark-dataframe-shuffling-can-hurt-your-partitioning-28d05fdcb6fa). [Learn more about Shuffle](https://sparkbyexamples.com/spark/spark-shuffle-partitions/).

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val nodeCount = sc.statusTracker.getExecutorInfos.length // for the entire cluster
# MAGIC val workerCount = nodeCount match {
# MAGIC   case 1 => 1 // if running a single-node cluster
# MAGIC   case x => x - 1 //  don't include the driver node
# MAGIC }
# MAGIC 
# MAGIC // Quick & Dirty Example: count the cores on your driver node, assuming the same instance type for worker nodes
# MAGIC val driverCoreCount = java.lang.Runtime.getRuntime.availableProcessors 
# MAGIC val workerCoreCount = workerCount * driverCoreCount
# MAGIC 
# MAGIC spark.conf.set("spark.sql.shuffle.partitions", workerCoreCount)

# COMMAND ----------

# MAGIC %md Hidden cells below: just setting up some toy data

# COMMAND ----------

# CHANGE ME to a unique team name
# create an internal metastore on DBFS (Databricks File System) to keep track of tables
db = "YOURAWESOMETEAMNAME"

assert db != "YOURAWESOMETEAMNAME", "You didn't read the instructions :) Please change the db to a unique team name"

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
spark.sql(f"USE {db}")

spark.sql("SET spark.databricks.delta.formatCheck.enabled = false")
spark.sql("SET spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true")

# COMMAND ----------

import random
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.types import *


def my_checkpoint_dir(): 
  return "/tmp/delta_demo/chkpt/%s" % str(random.randint(0, 10000))

# User-defined function to generate random state
@udf(returnType=StringType())
def random_state():
  return str(random.choice(["CA", "TX", "NY", "WA"]))


# Function to start a streaming query with a stream of randomly generated load data and append to the parquet table
def generate_and_append_data_stream(table_format, table_name, schema_ok=False, workload_source="structured-streaming"):
  """
  We're going to generate our own data from scratch using a simple "Rate Source".
  A Rate source generates data at the specified number of rows per second, each row containing the following columns:
    - timestamp (TimestampType)
    - value (LongType)
  Where timestamp is the time of message dispatch, and value is of Long type containing the message count, starting from 0 as the first row. 
  This source is intended for testing and benchmarking. You can also have a look at "Socket Source"
  
  https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
  """
  
  raw_stream_data = (
    spark
      .readStream
      .format("rate") # search for "Rate source" on this page https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
      .option("rowsPerSecond", 500)
      .load()
  )
  
  stream_data = (
    raw_stream_data
      .withColumn("loan_id", 10000 + F.col("value"))
      .withColumn("funded_amnt", (F.rand() * 5000 + 5000).cast("integer"))
      .withColumn("paid_amnt", F.col("funded_amnt") - (F.rand() * 2000))
      .withColumn("addr_state", random_state())
      .withColumn("workload_source", F.lit(workload_source))
  )
    
  if schema_ok:
    stream_data = stream_data.select("loan_id", "funded_amnt", "paid_amnt", "addr_state", "workload_source", "timestamp")
      
  query = (stream_data.writeStream
    .format(table_format)
    .option("checkpointLocation", my_checkpoint_dir())
    .trigger(processingTime = "5 seconds")
    .table(table_name))

  return query

# COMMAND ----------

import random
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.types import *


def my_checkpoint_dir(): 
  return "/tmp/delta_demo/chkpt/%s" % str(random.randint(0, 10000))

# User-defined function to generate random state
@udf(returnType=StringType())
def random_state():
  return str(random.choice(["CA", "TX", "NY", "WA"]))


# Function to start a streaming query with a stream of randomly generated load data and append to the parquet table
def generate_and_append_data_stream(table_format, table_name, schema_ok=False, workload_source="structured-streaming"):
  """
  We're going to generate our own data from scratch using a simple "Rate Source".
  A Rate source generates data at the specified number of rows per second, each row containing the following columns:
    - timestamp (TimestampType)
    - value (LongType)
  Where timestamp is the time of message dispatch, and value is of Long type containing the message count, starting from 0 as the first row. 
  This source is intended for testing and benchmarking. You can also have a look at "Socket Source"
  
  https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
  """
  
  raw_stream_data = (
    spark
      .readStream
      .format("rate") # search for "Rate source" on this page https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
      .option("rowsPerSecond", 500)
      .load()
  )
  
  stream_data = (
    raw_stream_data
      .withColumn("loan_id", 10000 + F.col("value"))
      .withColumn("funded_amnt", (F.rand() * 5000 + 5000).cast("integer"))
      .withColumn("paid_amnt", F.col("funded_amnt") - (F.rand() * 2000))
      .withColumn("addr_state", random_state())
      .withColumn("workload_source", F.lit(workload_source))
  )
    
  if schema_ok:
    stream_data = stream_data.select("loan_id", "funded_amnt", "paid_amnt", "addr_state", "workload_source", "timestamp")
      
  query = (stream_data.writeStream
    .format(table_format)
    .option("checkpointLocation", my_checkpoint_dir())
    .trigger(processingTime = "5 seconds")
    .table(table_name))

  return query

# COMMAND ----------

# Function to stop all streaming queries 
def stop_all_streams():
    print("Stopping all streams")
    for s in spark.streams.active:
        try:
            s.stop()
        except:
            pass
    print("Stopped all streams")
    dbutils.fs.rm("/tmp/delta_demo/chkpt/", True) # delete all checkpoints
    return

def cleanup_paths_and_tables():
    dbutils.fs.rm("/tmp/delta_demo/", True)
    dbutils.fs.rm("file:/dbfs/tmp/delta_demo/loans_parquet/", True)
        
    for table in [f"{db}.loans_parquet", f"{db}.loans_delta", f"{db}.loans_delta2"]:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    return
    
cleanup_paths_and_tables()

# COMMAND ----------

# MAGIC %sh mkdir -p /dbfs/tmp/delta_demo/loans_parquet/; wget -O /dbfs/tmp/delta_demo/loans_parquet/loans.parquet https://github.com/data-derp/small-exercises/blob/master/real-world-structured-streaming/loan-risks.snappy.parquet?raw=true

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Write a batch of data in to our (Delta Lake - more on that on Monday!) table

# COMMAND ----------

parquet_path = "file:/dbfs/tmp/delta_demo/loans_parquet/"

df = (spark.read.format("parquet").load(parquet_path)
      .withColumn("workload_source", F.lit("batch"))
      .withColumn("timestamp", F.current_timestamp()))

df.write.format("delta").mode("overwrite").saveAsTable("loans_delta") # save DataFrame as a registered table in your Databricks-managed metastore

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Write a data stream into our Delta Lake table

# COMMAND ----------

# Set up 2 streaming writes to our table
stream_runner = generate_and_append_data_stream(table_format="delta", table_name="loans_delta", schema_ok=True, workload_source='structured-streaming')

# COMMAND ----------

# MAGIC %md ### Create 2 continuous streaming readers of our Delta Lake table to illustrate streaming progress
# MAGIC Let's use the Loans example from the delta lake exercise...

# COMMAND ----------

# Streaming read #1
display(spark.readStream.format("delta").table("loans_delta").groupBy("workload_source").count().orderBy("workload_source"))

# COMMAND ----------

# Streaming read #2
display(spark.readStream.format("delta").table("loans_delta").groupBy("workload_source", F.window("timestamp", "10 seconds")).count().orderBy("window"))

# COMMAND ----------

dbutils.notebook.exit("stop") # breakpoint for Run All

# COMMAND ----------

stop_all_streams()

# COMMAND ----------

cleanup_paths_and_tables()

# COMMAND ----------

# MAGIC %md ...but can we do **more** than just boring `sum` and `count` aggregations for isolated micro-batches/windows?

# COMMAND ----------

# MAGIC %md ### Stateful Streams
# MAGIC 
# MAGIC <img src="http://33.media.tumblr.com/01122720f975effd9fd67f9239019ccf/tumblr_nu83n0Tihp1rcmk8po3_r1_500.gif" width=1012/>
# MAGIC <br>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Arbitrary Stateful Streaming (low-level)
# MAGIC - [Databricks Example](https://databricks.com/blog/2017/10/17/arbitrary-stateful-processing-in-apache-sparks-structured-streaming.html) <br>
# MAGIC - [flatMapGroupsWithState Example](https://jaceklaskowski.gitbooks.io/spark-structured-streaming/content/spark-sql-streaming-demo-arbitrary-stateful-streaming-aggregation-flatMapGroupsWithState.html)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Configure our Cluster

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // Cluster Configuration
# MAGIC val nodeCount = sc.statusTracker.getExecutorInfos.length // for the entire cluster
# MAGIC val workerCount = nodeCount match {
# MAGIC   case 1 => 1 // if running a single-node cluster
# MAGIC   case x => x - 1 //  don't include the driver node
# MAGIC }
# MAGIC 
# MAGIC // Quick & Dirty Calculation: count the cores on your driver node, assuming the same instance type for worker nodes
# MAGIC val driverCoreCount = java.lang.Runtime.getRuntime.availableProcessors 
# MAGIC val workerCoreCount = workerCount * driverCoreCount
# MAGIC 
# MAGIC spark.conf.set("spark.sql.shuffle.partitions", workerCoreCount) // optimize shuffle partitions

# COMMAND ----------

# MAGIC %md
# MAGIC #### START EXERCISE HERE: Problem Simulation
# MAGIC 
# MAGIC * **Given:**
# MAGIC   - The client company operates a bunch of IoT devices (use your imagination) and need to detect potential faults.
# MAGIC   - Each IoT device has a power status (`1 = ON` and `0 = OFF`)
# MAGIC   - The client wants to **visually monitor** the devices' raw signals to ensure smooth operation
# MAGIC   - However, the client also wants to **visualize key metrics** (e.g. how many flickers have happened in the past `x` minutes?)
# MAGIC * When: a signal has flickered too much (we won't define a threshold for this problem)
# MAGIC * Then: I need to send in a maintenance team. 
# MAGIC 
# MAGIC **Definition of Flickering**
# MAGIC - Changing from ON -> OFF -> ON in less than a certain time threshold

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Simulate Input Data as a Stream (nothing you need to understand/change)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import java.sql.Timestamp
# MAGIC import org.apache.spark.sql.{functions => F}
# MAGIC import org.apache.spark.sql.Column
# MAGIC 
# MAGIC case class Signal(deviceId: String, epoch: Long, voltage: Int)
# MAGIC val degradationRate = 0.001 // Simulate how fast a device wears out over time. Tune empirically, so that visualizations don't go too fast/slow
# MAGIC 
# MAGIC val signalsStream = spark
# MAGIC   .readStream
# MAGIC   .format("rate")
# MAGIC   .option("rowsPerSecond", 10)
# MAGIC   .load()
# MAGIC   .withColumn("deviceId", F.concat_ws("", F.lit("D"), (F.col("value") % 3).cast("int") + F.lit(1))) // 3 devices with ids: D1, D2, D3
# MAGIC   .withColumn("epoch", F.col("timestamp").cast("long")) // to UNIX seconds
# MAGIC   .withColumn("degradation", F.col("value").cast("double") * F.lit(degradationRate))
# MAGIC   .withColumn("incidenceProbability", F.when(F.col("deviceId") =!= F.lit("D1"), F.lit(0.01)).otherwise(F.lit(0.05) + F.col("degradation"))) // simulate degradation for deviceId: D1
# MAGIC   .withColumn("voltage", (F.rand() > F.col("incidenceProbability")).cast("int")) // generate a digital signal between 0 and 1 (with an 1% chance of being 0 for deviceIds: D2 & D3)
# MAGIC   .select("deviceId", "epoch", "voltage", "timestamp") // only these fields will be available to you

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Determine if there is Flickering
# MAGIC   - Since the definition of flickering involves custom logic, we propose using lower-level operations such as flatMapGroups (groupBy device Id)
# MAGIC   - Since the client wants to keep a **cumulative** count of incidents, we propose adding state -> flatMapGroupsWithState
# MAGIC   
# MAGIC ##### Logic & Implementation:
# MAGIC   - Develop a custom Scala function that can analyze each micro-batch as well as the state for each device Id
# MAGIC   - Identify changes in voltage going from 1 <-> 0, i.e. step changes
# MAGIC   - If the current step change is from 0 -> 1, the previous one must've been from 1 -> 0
# MAGIC   - Classify as flicker or non-flicker: compute the time difference between these two step changes
# MAGIC   - Finally, don't forget to check for a step change from the previous micro-batch
# MAGIC   
# MAGIC ##### Things to Consider
# MAGIC Your final plot should hopefully look **qualitatively similar** to the following picture:
# MAGIC - it is also possible that the plotting library can have a glitch, detach then reattach the notebook to the cluster and try again
# MAGIC 
# MAGIC <img src="https://github.com/kelseymok/data-derp/blob/62dfcee256494b698109f878ac1d8b4fcd2426bc/datasets/streaming-flicker-count.png?raw=true" width=512/>
# MAGIC <br>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Some Helpers
# MAGIC We've constructed some entities (as Scala case classes) to help you out.
# MAGIC There are namely:
# MAGIC 
# MAGIC **DeviceState**
# MAGIC - Important state information that we, *as developers of this streaming app*, **decided that we need to consider** per each device
# MAGIC - Note that it is completely up to us to define the data structure of our state
# MAGIC - We can create any arbitrary case class to store state using any representation, as long as it solves our problem
# MAGIC 
# MAGIC **StepChange**
# MAGIC - While the `Signal` case class represents the raw voltage data (having values 0 or 1), it's important for our problem to also track **changes**
# MAGIC - `StepChange` describes **how** and **when** a step change in the raw voltage happened, e.g. the raw voltage changed from 0->1 at a certain timestamp (in epoch seconds)
# MAGIC 
# MAGIC **FlickerReport**
# MAGIC - The schema of our stream's output that we, *as developers of this streaming app*, **decided that to output to the customer**
# MAGIC - Note that it is completely up to us to define the data structure of our output
# MAGIC - We can create any arbitrary case class to represent the output schema, as long as the information answers important business questions

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.streaming.GroupState
# MAGIC 
# MAGIC // Describes **how** and **when** a step change in the raw signal happened, e.g. the raw signal changed from 0->1 at a certain timestamp (in epoch seconds)
# MAGIC // direction can either be 1 (implies 0->1) or -1 (implies 1->0)
# MAGIC // epoch is in UNIX seconds
# MAGIC case class StepChange(direction: Int, epoch: Long) 
# MAGIC 
# MAGIC // Important state information that we, *as developers of this streaming app*, **decided that we need to consider** per each device
# MAGIC // the Signal case class was defined when we created signalsStream
# MAGIC case class DeviceState(
# MAGIC   deviceId: String, 
# MAGIC   previousSignal: Option[Signal], // Store the last Signal value from the previous micro-batch 
# MAGIC   previousStepChange: Option[StepChange], // Store the last StepChange from the previous micro-batch 
# MAGIC   flickerCount: Int // Accumulate flicker count indefinitely (for this example)
# MAGIC )
# MAGIC 
# MAGIC // What our streaming analytics job will output to the customer
# MAGIC // epoch should be the latest timestamp (in UNIX seconds) of the current micro-batch
# MAGIC // flickerCount should be the total number of flickers counted so far (since the streaming app was launched), not just for the current micro-batch
# MAGIC case class FlickerReport(deviceId: String, epoch: Long, flickerCount: Int) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### TODO: Now it's your turn! 
# MAGIC 
# MAGIC - Streaming frameworks have **lots** of different flavours and setups
# MAGIC - As it might not be very useful to go deep into one particular framework, we want to give you some basic hands-on skills in developing **streaming logic & applications** in general
# MAGIC - In the real world, you will have to deal with out-of-orderness (late messages), low-level stream manipulation logic, state management
# MAGIC - Hopefully, this gives you some appreciation for Functional Programming principles for tackling these problems
# MAGIC   - concepts such as **pattern matching**, filter, map, flatMap, collections (incl. "sliding windows") are **widely used in practice**

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC def detectStepChanges(signalSeq: Seq[Signal]): List[StepChange] = {
# MAGIC   /* TODO: Detect the StepChanges in the current micro-batch
# MAGIC      Signals => 1 -> 1 -> 1 -> 0 -> 1 -> ...
# MAGIC      Only consider changes from (1 -> 0) or (0 -> 1)
# MAGIC      Ignore 1 -> 1 or 0 -> 0 
# MAGIC      
# MAGIC      - the epoch for each StepChange should correspond to when the raw signal ACTUALLY CHANGED
# MAGIC        e.g. if going from 1 -> 0, take the timestamp where the raw signal first went to 0
# MAGIC   */
# MAGIC   signalSeq.size match {
# MAGIC     case n if (n < 2) => List() // if list size less than 2, don't even bother!
# MAGIC     case _ => {
# MAGIC       signalSeq.sliding(2).flatMap(
# MAGIC       pair => 
# MAGIC         if (pair(0).voltage != pair(1).voltage) { // Only consider changes from (1 -> 0) or (0 -> 1)
# MAGIC           val direction = pair(1).voltage - pair(0).voltage
# MAGIC           val epoch = pair(1).epoch
# MAGIC           List(StepChange(direction=direction, epoch))
# MAGIC           } 
# MAGIC         else List() // Ignore 1 -> 1 or 0 -> 0 
# MAGIC       ).toList
# MAGIC     }
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC val exampleSignalSeq: Seq[Signal] = List(
# MAGIC   Signal(deviceId="XYZ", epoch=1L, voltage=1),  
# MAGIC   Signal(deviceId="XYZ", epoch=2L, voltage=1),  
# MAGIC   Signal(deviceId="XYZ", epoch=3L, voltage=0),  
# MAGIC   Signal(deviceId="XYZ", epoch=4L, voltage=1),  
# MAGIC   Signal(deviceId="XYZ", epoch=5L, voltage=1),  
# MAGIC )
# MAGIC assert(
# MAGIC   // test your logic here :)
# MAGIC   detectStepChanges(exampleSignalSeq) == List(StepChange(-1, 3L), StepChange(1, 4L))
# MAGIC )
# MAGIC 
# MAGIC def countNewFlickers(stepChanges: List[StepChange], FlickerTimeThreshold: Long = 1L): Int = {
# MAGIC   /* TODO: For each consecutive pair of StepChanges in the current micro-batch: 
# MAGIC      - we want to only consider those going from 1 -> 0 -> 1 (i.e. StepChange with direction = -1, followed by a StepChange direction = 1)
# MAGIC      - if matching this pattern, check for the time difference
# MAGIC      - if the difference in time between the two StepChanges is less than or equal to FlickerTimeThreshold
# MAGIC      - count up all events that match this criteria
# MAGIC   */
# MAGIC   stepChanges.size match {
# MAGIC     case n if (n < 2) => 0 // if list size less than 2, don't even bother!
# MAGIC     case _ => {
# MAGIC       val FlickerTimeThreshold: Long = 1L // (in seconds) as defined by client
# MAGIC       stepChanges.sliding(2).map(
# MAGIC         pair => if (
# MAGIC           (pair(1).direction == 1) && // if it's flickered back ON
# MAGIC           (pair(1).epoch - pair(0).epoch <= FlickerTimeThreshold) // if short amount of time is a flicker, if long amount of time, you could have intentionally turned off device
# MAGIC         ) 1 else 0
# MAGIC       ).sum
# MAGIC     }
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC assert(
# MAGIC   // test your logic here :)
# MAGIC   countNewFlickers(detectStepChanges(exampleSignalSeq)) == 1
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Putting it all together

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.streaming.GroupState
# MAGIC 
# MAGIC def countFlickersPerDevice(deviceId: String, signals: Iterator[Signal], state: GroupState[DeviceState]): Iterator[FlickerReport] = { // GroupState is provided by Spark
# MAGIC   // Retrieve state if existing, otherwise initialize
# MAGIC   val initialState: DeviceState = DeviceState(
# MAGIC     deviceId = deviceId,
# MAGIC     previousSignal = None,
# MAGIC     previousStepChange = None,
# MAGIC     flickerCount = 0
# MAGIC   )
# MAGIC   
# MAGIC   val oldState = state.getOption.getOrElse(initialState)
# MAGIC 
# MAGIC   // Load the raw signal coming in
# MAGIC   // if there is some previous state for previousSignal, prepend it to the sequence
# MAGIC   // otherwise, just use the current micro-batch's Signals
# MAGIC   val signalSeq: Seq[Signal] = oldState.previousSignal match {
# MAGIC     case Some(x) => (x +: signals.toSeq).sortBy(s => s.epoch) // if non-empty, prepend last voltage from the previous micro-batch
# MAGIC     case _ => signals.toSeq.sortBy(s => s.epoch) // otherwise, just use the current micro-batch
# MAGIC   }
# MAGIC   
# MAGIC   // Detect the StepChanges
# MAGIC   // Signals => 1 -> 1 -> 1 -> 0 -> 1 -> ...
# MAGIC   // Only consider changes from (1 -> 0) or (0 -> 1)
# MAGIC   // Ignore 1 -> 1 or 0 -> 0 
# MAGIC   val newStepChanges: List[StepChange] = detectStepChanges(signalSeq)  // TODO: implement detectStepChanges function in cell above
# MAGIC   
# MAGIC   val stepChanges: List[StepChange] = oldState.previousStepChange match {
# MAGIC     case Some(x) => (x +: newStepChanges).sortBy(s => s.epoch) // if non-empty, prepend last StepChange from the previous micro-batch
# MAGIC     case _ => (newStepChanges).sortBy(s => s.epoch) // otherwise, just use the current micro-match
# MAGIC   }
# MAGIC   
# MAGIC   // Find the latest StepChange, we'll need to store that in our state
# MAGIC   // if this micro-batch never flickered, just use the last state of the micro-batch
# MAGIC   val latestStepChange: Option[StepChange] = stepChanges match {
# MAGIC     case Nil => oldState.previousStepChange
# MAGIC     case _ => Some(stepChanges.last)
# MAGIC   }
# MAGIC   
# MAGIC   val FlickerTimeThreshold: Long = 1L // (in seconds) as defined by client
# MAGIC   val newFlickers: Int = countNewFlickers(stepChanges, FlickerTimeThreshold) // TODO: implement countNewFlickers function in cell above
# MAGIC 
# MAGIC   // Prepare to update state with latest info
# MAGIC   val latestSignal: Option[Signal] = signalSeq match {
# MAGIC     case Nil => oldState.previousSignal // if we didn't receive any raw signal values in the current micro-batch
# MAGIC     case _ => Some(signalSeq.last)
# MAGIC   }
# MAGIC   
# MAGIC   // Update state, so that the next micro-batch is aware of:
# MAGIC   //   - the latest Signal and StepChange that occurred in the previous micro-batch
# MAGIC   //   - the total flickerCount for that device so far
# MAGIC   val oldFlickers: Int = oldState.flickerCount
# MAGIC   val totalFlickers: Int = oldFlickers + newFlickers
# MAGIC   val newState: DeviceState = oldState.copy(flickerCount=totalFlickers, previousSignal=latestSignal, previousStepChange=latestStepChange)
# MAGIC   state.update(newState)
# MAGIC   // Optional, set a Time-To-Live or Timeout on state
# MAGIC   state.setTimeoutDuration("10 minutes")
# MAGIC 
# MAGIC   // Finally, return an iterator as the output for the current micro-batch
# MAGIC   val latestEpoch: Long = signalSeq.last.epoch
# MAGIC   Iterator(FlickerReport(deviceId, latestEpoch, totalFlickers))
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read Streaming and Transform using countFlickersPerDevice

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // stream processing using flatMapGroupsWithState operator
# MAGIC import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
# MAGIC 
# MAGIC val deviceId: Signal => String = {case Signal(deviceId, _, _) => deviceId} // pattern-match each Signal record to extract device Id
# MAGIC 
# MAGIC val flickerStream = signalsStream
# MAGIC   .as[Signal] // convert to our custom data structure for convenience
# MAGIC   .groupByKey(deviceId)
# MAGIC   .flatMapGroupsWithState(
# MAGIC     outputMode = OutputMode.Append,
# MAGIC     timeoutConf = GroupStateTimeout.ProcessingTimeTimeout
# MAGIC   )(countFlickersPerDevice)
# MAGIC   .withColumn("timestamp", F.from_utc_timestamp(F.col("epoch").cast("timestamp"), "UTC")) // in case we want to customize the timezone
# MAGIC   .drop("epoch")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Plot the Flickers (on/off)

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.streaming.Trigger
# MAGIC 
# MAGIC display(signalsStream.filter(F.col("deviceId") === F.lit("D1")), trigger=Trigger.ProcessingTime("5 seconds")) // let's visualize one of these devices

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Plot the Total Number of Flickers

# COMMAND ----------

# MAGIC %scala 
# MAGIC import org.apache.spark.sql.streaming.Trigger
# MAGIC 
# MAGIC display(flickerStream, trigger=Trigger.ProcessingTime("5 seconds"))

# COMMAND ----------

dbutils.notebook.exit("stop") # sets a breakpoint, avoids running cells below if you click Run All

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Cleanup

# COMMAND ----------

# Function to stop all streaming queries 
def stop_all_streams():
    print("Stopping all streams")
    for s in spark.streams.active:
        try:
            s.stop()
        except:
            pass
    print("Stopped all streams")
    dbutils.fs.rm("/tmp/delta_demo/chkpt/", True) # delete all checkpoints
    return

# COMMAND ----------

stop_all_streams()

# COMMAND ----------

spark.sql(f"DROP SCHEMA IF EXISTS {db}")

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC #### OK, awesome! so what are the limitations?
# MAGIC 
# MAGIC **State Management**
# MAGIC - How can I control total state size in my entire job?
# MAGIC - How can I control state time-to-live (TTL) in a **dynamic** way?
# MAGIC - What if I need something like a **FIFO queue** as my state? For example: <br> 
# MAGIC   1. Append the transaction to the list latest transactions, **without** always updating the **entire list**
# MAGIC   2. Keep only transactions from the past hour, **without** always updating the **entire list**
# MAGIC 
# MAGIC **Micro-Batch vs Continuous Processing**
# MAGIC - Latency: what if I need ultra low-latency (~in the milliseconds)
# MAGIC - Timers: what if I need some logic such as: *trigger an event **immediately** if I **haven't heard** from my IoT device for 123.456 seconds*

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC #### Introducing Flink
# MAGIC - Built specifically for stateful streaming
# MAGIC - Managed AWS service via Kinesis Data Analytics
# MAGIC - Mention Kappa Architecture and future unification of APIs (e.g. Apache Beam)
