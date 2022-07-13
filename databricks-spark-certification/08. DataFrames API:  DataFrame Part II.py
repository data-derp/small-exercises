# Databricks notebook source
# DBTITLE 1,Joins
# MAGIC %md
# MAGIC 1. **Inner joins**: Output is the records where the join "key" matches both DataFrames
# MAGIC 2. **Outer joins**: Output is union of records in both DataFrame. 
# MAGIC 3. **Left and right outer joins**:  Left outer join only keeps the results of outer join from the left dataframe, Right outer join works the same way but for the right DataFrame
# MAGIC 4. **Left semi joins**: Left semi join will only keep the values that match the right data frame keys
# MAGIC 5. **Left anti joins**: Left Anti joins are opposite of left semi joins, will only keep the values that dont match the right data frame keys
# MAGIC 6. **Cross joins**: Cross joins are equivalent to cross products on the keys

# COMMAND ----------

# MAGIC %run ./init

# COMMAND ----------

fire_incidents = "/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-incidents.csv"
fire_calls = "/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"

# COMMAND ----------

import pyspark.sql.functions as f

#Reading the dataset
df_fire_incidents = spark.read\
  .format("csv")\
  .option("header", True)\
  .option("inferSchema", True)\
  .load(fire_incidents)\
  .filter(f.col('Incident Number').isNotNull())\
  .dropDuplicates(subset = ['Incident Number'])
df_fire_incidents.cache()
display(df_fire_incidents)

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 _Dropping duplicates and null records to make the Join examples simpler, in real life scenario you have to be more judicious about this decision_

# COMMAND ----------

df_fire_calls = spark.read\
  .format("csv")\
  .option("header", True)\
  .option("inferSchema", True)\
  .load(fire_calls)\
  .filter(f.col('Incident Number').isNotNull())\
  .dropDuplicates(subset = ['Incident Number'])
df_fire_calls.cache()
display(df_fire_calls)

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Inner joins

# COMMAND ----------

inner_join_df = df_fire_incidents.alias("df1")\
  .join(
    df_fire_calls.alias("df2"), 
    f.col("df1.`Incident Number`") == f.col("df2.`Incident Number`"), 
    "inner"
  )
# Displaying a column from df1 and corresponding column from df2 based on the join
display(inner_join_df.select("df1.`Primary Situation`", "df2.CallType"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Outer joins

# COMMAND ----------

outer_join_df = df_fire_incidents.alias("df1")\
  .join(
    df_fire_calls.alias("df2"), 
    f.col("df1.`Incident Number`") == f.col("df2.`Incident Number`"), 
    "outer"
  )
# Display Incident number that is NOT present in df1 but present in df2
display(outer_join_df.filter(f.col("df1.`Incident Number`").isNull()).select('df2.`Incident Number`').limit(1))


# COMMAND ----------

# MAGIC %md
# MAGIC  💡  _Records where a "key" is present in one DataFrame but not the other are replaced with "Null"_

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Left and Right outer joins

# COMMAND ----------

'''
Left outer join only keeps the results of outer join from the left dataframe 
'''
left_outer_join_df = df_fire_incidents.alias("df1")\
  .join(
    df_fire_calls.alias("df2"), 
    f.col("df1.`Incident Number`") == f.col("df2.`Incident Number`"), 
    "left_outer"
  )

assert df_fire_incidents.count() == left_outer_join_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 _The assert statement is only valid if there is no key matching that also has duplicates_

# COMMAND ----------

'''
Similarly, Right outer join only keeps the results of outer join from the right dataframe 
'''
right_outer_join_df = df_fire_incidents.alias("df1")\
  .join(
    df_fire_calls.alias("df2"), 
    f.col("df1.`Incident Number`") == f.col("df2.`Incident Number`"), 
    "right_outer"
  )

assert df_fire_calls.count() == right_outer_join_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Left Semi Joins

# COMMAND ----------

'''
Left semi join will only keep the values that match the right data frame keys
Let's see how many fire_calls where recorded in dataframe fire_incidents
'''

left_semi_join_df = df_fire_calls.alias("df1")\
  .join(
    df_fire_incidents.alias("df2"), 
    f.col("df1.`Incident Number`") == f.col("df2.`Incident Number`"), 
    "left_semi"
  )
print(f"Total number of rows: {df_fire_calls.count()}")
print(f"Number of rows that match the key right dataframe {left_semi_join_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Left Anti Joins

# COMMAND ----------

'''
Left Anti joins are opposite of left semi joins, will only keep the values that dont
match the right data frame keys
'''

left_anti_join_df = df_fire_calls.alias("df1")\
  .join(
    df_fire_incidents.alias("df2"), 
    f.col("df1.`Incident Number`") == f.col("df2.`Incident Number`"), 
    "left_anti"
  )

total = df_fire_calls.count()
left_semi = left_semi_join_df.count()
left_anti = left_anti_join_df.count()
print(f"Total number of rows: {total}")
print(f"Number of rows that match the key right dataframe {left_semi}")
print(f"Number of rows that dont match the key right dataframe {left_anti}")

assert left_semi + left_anti == total

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 _Left anti joins are particularly useful if you are working with constantly updating table(in technical terms called a Type 2 dimension). It can help you track any insertions that happened between the current and old version of the table_

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Cross Joins

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 _The number of rows that result from a cross join is N X M where N and M are number of rows in left and right dataframe respectivley. Thus, we will create a small dataset to demonstrate it_

# COMMAND ----------

'''
Cross joins are equivalent to cross products on the keys i.e they will generate a row for every combination of keys in left and right dataFrame respectivley
'''

employee = spark.createDataFrame([
(0, "Sidharth Singh", 0, [100]),
(1, "Ishan Verma", 1, [500, 250, 100]),
(2, "Manu Mennon", 2, [250, 100])])\
.toDF("id", "name", "department", "whatsapp_group_id")
department = spark.createDataFrame([
(0, "floor 1", "Engineering"),
(2, "floor 2", "Delivery"),
(1, "floor 3", "Research")])\
.toDF("id", "floor", "department")
group = spark.createDataFrame([
(500, "IT"),
(250, "Administration"),
(100, "Delivery")])\
.toDF("id", "group_name")

# COMMAND ----------

display(department.crossJoin(employee))

# COMMAND ----------

display(df_fire_calls.filter(f.col('Incident Number') == 16084524))

# COMMAND ----------

# DBTITLE 1,Exercise
# MAGIC %md
# MAGIC **Scenario 8.1**
# MAGIC <br>**Dataset**: /databricks-datasets/learning-spark-v2/sf-fire
# MAGIC <br>**Problem Statement**: How many calls that were tagged 'false call/ false alarm' were treated as heighest priority and how many were treated as any other priority?
# MAGIC <br>**Steps**
# MAGIC 1. Read the dataset in dataFrames (remove duplicates(Incident numbers) and null rows)
# MAGIC 2. From df_fire_incidents filter all records where "Primary Situation" column has "False" as substring
# MAGIC 3. From df_fire_calls filter all records where "OrigPriority" is 3
# MAGIC 4. Join the resultant coulmns and get a count

# COMMAND ----------

'''
Write code here
'''

# COMMAND ----------

# MAGIC %md
# MAGIC **Solution (hidden)**

# COMMAND ----------

fire_incidents = "/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-incidents.csv"
fire_calls = "/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"

# COMMAND ----------

#step 1
import pyspark.sql.functions as f
df_fire_incidents = spark.read\
  .format("csv")\
  .option("header", True)\
  .option("inferSchema", True)\
  .load(fire_incidents)\
  .filter(f.col('Incident Number').isNotNull())\
  .dropDuplicates(subset = ['Incident Number'])
df_fire_incidents.cache()

df_fire_calls = spark.read\
  .format("csv")\
  .option("header", True)\
  .option("inferSchema", True)\
  .load(fire_calls)\
  .filter(f.col('Incident Number').isNotNull())\
  .dropDuplicates(subset = ['Incident Number'])
df_fire_calls.cache()

# COMMAND ----------

from pyspark.sql import functions as f
#step 2
primary_situation_false = df_fire_incidents.filter(f.lower(f.col("Primary Situation")).contains("false"))
#step 3
orig_priority_3 = df_fire_calls.filter(f.col("OrigPriority")==3)

# COMMAND ----------

#step 4
inner_join_df = orig_priority_3.alias("df1")\
  .join(
    primary_situation_false.alias("df2"), 
    f.col("df1.`Incident Number`") == f.col("df2.`Incident Number`"), 
    "inner"
  )
#step 5 Try to find the second part to the question (count) yourself ;)

# COMMAND ----------

# DBTITLE 1,References
# MAGIC %md
# MAGIC 1) [Type 2 dimensions](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row)