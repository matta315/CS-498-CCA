#!/usr/bin/env python
import math
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def get_sum(list, avg):
       sum = 0
       for i in list:
              sum += (i - avg // 1)** 2
       return sum


conf = SparkConf().setMaster("local").setAppName("TopTitleStatistics")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf = conf)

lines = sc.textFile(sys.argv[1],1)

# to have RDD.toDF function, you need to implicitly have a spark session
spark = SparkSession(sc)

# Coverting RDD to Dataframe with 1 column
fixed_rdd = lines.map(lambda x: Row(x))

# corresponding schema
sch = StructType([StructField('inputLine', StringType(), True)])

# then finally, convert fixed_rdd to a DF
df = fixed_rdd.toDF(schema=sch)

# Split dataframe into 2 columns: word & Value
df2 = df.withColumn('word', split(df['inputLine'],'\t').getItem(0))\
       .withColumn ('count', split(df['inputLine'],'\t').getItem(1).cast(DoubleType()))

# Calculation with Spark
count = df2.count()
total = df2.select(sum(df2["count"])).collect()[0]['sum(count)']
avg = df2.select(mean(df2["count"])).collect()[0]['avg(count)']
max = df2.select(max(df2["count"])).collect()[0]['max(count)']
min = df2.select(min(df2["count"])).collect()[0]['min(count)']

list_count = df2.select('count').rdd.map(lambda row : row[0]).collect()

var = get_sum(list_count, avg) // count

with open(sys.argv[2],"w") as outputFile:
       outputFile.write('Mean\t%s\n' % int(avg // 1))
       outputFile.write('Sum\t%s\n' % int(total // 1))
       outputFile.write('Min\t%s\n' % int(min // 1))
       outputFile.write('Max\t%s\n' % int(max // 1))
       outputFile.write('Var\t%s\n' % int(var))

'''
TODO write your output here
write results to output file. Format
outputFile.write('Mean\t%s\n' % ans1)
outputFile.write('Sum\t%s\n' % ans2)
outputFile.write('Min\t%s\n' % ans3)
outputFile.write('Max\t%s\n' % ans4)
outputFile.write('Var\t%s\n' % ans5)
'''

sc.stop()

