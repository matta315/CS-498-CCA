#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, trim, udf, lit, explode
from pyspark.sql.types import StructType, StructField, StringType, Row, ArrayType, IntegerType

conf = SparkConf().setMaster("local").setAppName("OrphanPages")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf = conf)

# Help Function:


def remove_selflink(link, connectedLink):
    if link in connectedLink:
        connectedLink.remove(link)
    return connectedLink


# START:


lines = sc.textFile(sys.argv[1], 1)

# to have RDD.toDF function, you need to implicitly have a spark session
spark = SparkSession(sc)

# Coverting RDD to Dataframe with 1 column
fixed_rdd = lines.map(lambda x: Row(x))

# corresponding schema
sch = StructType([StructField('inputLine', StringType(), True)])

# then finally, convert fixed_rdd to a DF
df = fixed_rdd.toDF(schema=sch)

# Split output into dataframe with 2 column
df2 = df.withColumn('page', split(df['inputLine'],':').getItem(0).cast(IntegerType()))\
       .withColumn ('connectedPage', split(df['inputLine'],':').getItem(1))\
        .drop('inputLine')

df2 = df2.withColumn(
    'connectedPage',
    split(trim(col("connectedPage")), " ")
        .cast(ArrayType(IntegerType()))
    )

# udf - remove page that links to itself
df_clean = df2.withColumn(
    'connectedPage',
    udf(remove_selflink, ArrayType(IntegerType())) (col('page'),col('connectedPage'))
)

# Create 2 data frame to track on connection
df_page = df_clean.select('page').withColumn('count',lit(0)).distinct()

df_connectedPage = df_clean.select('connectedPage').withColumn("connectedPage", explode(col('connectedPage'))) \
                           .withColumn('count', lit(1)).distinct()

# combine 2 dataframe
df_orphans = df_page.join(df_connectedPage, df_page.page == df_connectedPage.connectedPage, how='leftanti')

result = df_orphans.select('page').rdd.map(lambda row : row[0]).collect()
result = [int(i) for i in result]
result = sorted(result)

with open(sys.argv[2], "w") as output:
    [output.write(str(line) + "\n") for line in result]
#write results to output file. Foramt for each line: (line+"\n")

sc.stop()