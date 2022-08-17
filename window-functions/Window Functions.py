# Databricks notebook source
# MAGIC %md
# MAGIC [Notebook Source](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/968100988546031/157591980591166/8836542754149149/latest.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Window Functions
# MAGIC 
# MAGIC Pyspark window functions are useful when you want to examine relationships within groups of data rather than between groups of data (as for groupBy)
# MAGIC 
# MAGIC To use them you start by defining a window function then select a separate function or set of functions to operate within that window

# COMMAND ----------

import pandas as pd
import pyspark.sql.functions as fn
from pyspark.sql import SparkSession
from pyspark.sql import Window

# COMMAND ----------

# Create a spark session
spark_session = SparkSession.builder.getOrCreate()

# lets define a demonstration DataFrame to work on
df_data = {'partition': ['a','a', 'a', 'a', 'b', 'b', 'b', 'c', 'c',],
           'col_1': [1,1,1,1,2,2,2,3,3,], 
           'aggregation': [1,2,3,4,5,6,7,8,9,],
           'ranking': [4,3,2,1,1,1,3,1,5,],
           'lagging': [9,8,7,6,5,4,3,2,1,],
           'cumulative': [1,2,4,6,1,1,1,20,30,],
          }
df_pandas = pd.DataFrame.from_dict(df_data)
# create spark dataframe
df = spark_session.createDataFrame(df_pandas)

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simple aggregation functions
# MAGIC 
# MAGIC we can use the standard group by aggregations with window functions. These functions use the simplest form of window which just defines grouping

# COMMAND ----------

# aggregation functions use the simplest form of window which just defines grouping
aggregation_window = Window.partitionBy('partition')

# then we can use this window function for our aggregations
df_aggregations = df.select(
  'partition', 'aggregation'
).withColumn(
  'aggregation_sum', fn.sum('aggregation').over(aggregation_window),
).withColumn(
  'aggregation_avg', fn.avg('aggregation').over(aggregation_window),
).withColumn(
  'aggregation_min', fn.min('aggregation').over(aggregation_window),
).withColumn(
  'aggregation_max', fn.max('aggregation').over(aggregation_window),
)

df_aggregations.show()
# note that after this operation the row order of display within the dataframe may have changed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row wise ordering and ranking functions
# MAGIC 
# MAGIC We can also use window funtions to order and rank data. These functions add an element to the definition of the window which defines both grouping AND ordering

# COMMAND ----------

# lets define a ranking window
ranking_window = Window.partitionBy('partition').orderBy('ranking')

df_ranks = df.select(
  'partition', 'ranking'
).withColumn(
  # note that fn.row_number() does not take any arguments
  'ranking_row_number', fn.row_number().over(ranking_window) 
).withColumn(
  # rank will leave spaces in ranking to account for preceding rows receiving equal ranks
  'ranking_rank', fn.rank().over(ranking_window)
).withColumn(
  # dense rank does not account for previous equal rankings
  'ranking_dense_rank', fn.dense_rank().over(ranking_window)
).withColumn(
  # percent rank ranges between 0-1 not 0-100
  'ranking_percent_rank', fn.percent_rank().over(ranking_window)
).withColumn(
  # fn.ntile takes a parameter for now many 'buckets' to divide rows into when ranking
  'ranking_ntile_rank', fn.ntile(2).over(ranking_window)
)

df_ranks.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating lagged columns
# MAGIC 
# MAGIC If we want to conduct operations like calculating the difference between subsequent operations in a group, we can use window functions to create the lagged values we require to perform the calculation. Where there is no preceding lag value, a null entry will be inserted not a zero.
# MAGIC 
# MAGIC The inverse of lag is lead. Effectively fn.lag(n) == fn.lead(-n)

# COMMAND ----------

lag_window = Window.partitionBy('partition').orderBy('lagging')

df_lagged = df.select(
  'partition', 'lagging'
).withColumn(
  # note that lag requires both column and lag amount to be specified
  # It is possible to lag a column which was not the orderBy column
  'lagging_lag_1', fn.lag('lagging', 1).over(lag_window)
).withColumn(
  'lagging_lag_2', fn.lag('lagging', 2).over(lag_window)
).withColumn(
  'lagging_lead_1', fn.lead('lagging', 1).over(lag_window)
).withColumn(
  # note how 'lagging_lag_1' == 'lagging_lead_minus_1'
  'lagging_lead_minus_1', fn.lead('lagging', -1).over(lag_window)
).withColumn(
  # we can also perform calculations between lagged and unlagged columns of course
  'difference_between', fn.col('lagging') - fn.lag('lagging', 1).over(lag_window)
)

df_lagged.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cumulative Calculations (Running totals and averages)
# MAGIC 
# MAGIC There are often good reasons to want to create a running total or running average column. In some cases we might want running totals for subsets of data. Window functions can be useful for that sort of thing. 
# MAGIC 
# MAGIC In order to calculate such things we need to add yet another element to the window. Now we account for partition, order and which rows should be covered by the function. This can be done in two ways we can use **rangeBetween** to define how similar values in the window must be to be considered, or we can use **rowsBetween** to define how many rows should be considered. The current row is considered row zero, the following rows are numbered positively and the preceding rows negatively. For cumulative calculations you can define "all previous rows" with **Window.unboundedPreceding** and "all following rows" with **Window.unboundedFolowing**
# MAGIC 
# MAGIC Note that the window may vary in size as it progresses over the rows since at the start and end part of the window may "extend past" the existing rows

# COMMAND ----------

#suppose we want to average over the previous, current and next values
# running calculations need a more complicated window as shown here
cumulative_window_1 = Window.partitionBy(
  'partition'
).orderBy(
  'cumulative'
# for a rolling average lets use rowsBetween
).rowsBetween(
  -1,1
)

df_cumulative_1 = df.select(
  'partition', 'cumulative'
).withColumn(
  'cumulative_avg', fn.avg('cumulative').over(cumulative_window_1)
)

df_cumulative_1.show()
# note how the averages don't use 3 rows at the ends of the window

# COMMAND ----------

# running totals also require a more complicated window as here. 
cumulative_window_2 = Window.partitionBy(
  'partition'
).orderBy(
  'cumulative'
# in this case we will use rangeBetween for the sum
).rangeBetween(
# In this case we need to use Window.unboundedPreceding to catch all earlier rows
  Window.unboundedPreceding, 0
)

df_cumulative_2 = df.select(
  'partition', 'cumulative'
).withColumn(
  'cumulative_sum', fn.sum('cumulative').over(cumulative_window_2)
)

df_cumulative_2.show()
# note the summing behaviour where multiple identical values are present in the orderBy column

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combining Windows and Calling Different Columns
# MAGIC It is also possible to combine windows and also to call windows on columns other than the ordering column. These more advanced uses can require careful thought to ensure you achieve the intended results

# COMMAND ----------

# we can make a window function equivalent to a standard groupBy:

# first define two windows
aggregation_window = Window.partitionBy('partition')
grouping_window = Window.partitionBy('partition').orderBy('aggregation')

# then we can use this window function for our aggregations
df_aggregations = df.select(
  'partition', 'aggregation'
).withColumn(
  # note that we calculate row number over the grouping_window
  'group_rank', fn.row_number().over(grouping_window) 
).withColumn(
  # but we calculate other columns over the aggregation_window
  'aggregation_sum', fn.sum('aggregation').over(aggregation_window),
).withColumn(
  'aggregation_avg', fn.avg('aggregation').over(aggregation_window),
).withColumn(
  'aggregation_min', fn.min('aggregation').over(aggregation_window),
).withColumn(
  'aggregation_max', fn.max('aggregation').over(aggregation_window),
).where(
  fn.col('group_rank') == 1
).select(
  'partition', 
  'aggregation_sum', 
  'aggregation_avg', 
  'aggregation_min', 
  'aggregation_max'
)

df_aggregations.show()

# this is equivalent to the rather simpler expression below
df_groupby = df.select(
  'partition', 'aggregation'
).groupBy(
  'partition'
).agg(
  fn.sum('aggregation').alias('aggregation_sum'),
  fn.avg('aggregation').alias('aggregation_avg'),
  fn.min('aggregation').alias('aggregation_min'),
  fn.max('aggregation').alias('aggregation_max'),
)

df_groupby.show()

# COMMAND ----------

# in some cases we can create a window on one column but use the window on another column 
# note that only functions where the column is specified allow this
lag_window = Window.partitionBy('partition').orderBy('lagging')

df_cumulative_2 = df.select(
  'partition', 'lagging', 'cumulative',
).withColumn(
  'lag_the_laggging_col', fn.lag('lagging', 1).over(lag_window)
).withColumn(
  # It is possible to lag a column which was not the orderBy column
  'lag_the_cumulative_col', fn.lag('cumulative', 1).over(lag_window)
)

df_cumulative_2.show()
