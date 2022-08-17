# Databricks notebook source
import pandas as pd
import random
import uuid
import string
import json
from numpy import arange

# COMMAND ----------

'''
Creating data with different delimiter
'''
# file_path = 'dbfs:/databricks-datasets/flights/departuredelays.csv'
file_path = '/databricks-datasets/flights/departuredelays.csv'

data = spark.read.format("csv").option("header", True).load(file_path).toPandas()

data.to_csv(f"/dbfs{working_directory}/df_some_sep", header=True, index=False, sep='$') # this is a csv file

# COMMAND ----------

# creating dummy json data
def create_row():
  name = random.choice(['Sidharth', 'Manu', 'Akshay', 'Ganesh', 'Jebby', 'John', 'Pavan', 'Teja']) + ' ' + random.choice(string.ascii_uppercase) + '.'
  subjects = random.sample(['Physics', 'Chemsitry', 'Mathematics', 'Economics', 'Biology', 'English', 'Hindi', 'Information Technology'], 3)
  street = str(random.choice(range(1,100))) + ' ' + random.choice(['street', 'drive', 'lane'])
  house = random.choice(range(1,100))
  city = random.choice(['Delhi', 'Ghaziabad', 'Bengalore', 'Pune'])
  total_days_present = random.choice(range(50,100))
  total_marks = random.choice(arange(70,95, 0.5))
  return {
    'id':uuid.uuid1().int,
    'name':name,
    'subjects':subjects,
    'address':{
      'street':street,
      'house':house,
      'city':city
    },
    "total_days_present":total_days_present,
    "total_days":100,
    "total_marks":total_marks
  }

output = []

for i in range(0,1000):
  output.append(json.dumps(create_row()))

with open(f'/dbfs/{working_directory}/student.json', 'w') as fl:
  for data in output:
    fl.write(data + '\n')


# COMMAND ----------

file_path = f"{working_directory}/student.json"

df = spark.read.json(file_path)

#Examining Schema
df.printSchema()

# COMMAND ----------

#creating temp directory
import os
dir_name = f"/dbfs{working_directory}/compression"

if not os.path.exists(dir_name):
  os.mkdir(dir_name)