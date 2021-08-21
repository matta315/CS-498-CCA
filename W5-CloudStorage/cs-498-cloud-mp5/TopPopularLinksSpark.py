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
    udf(remove_selflink, ArrayType(IntegerType()))(col('page'), col('connectedPage'))
)

# Assigning connection to page
df_connectedPage = df_clean.select('connectedPage').withColumn("connectedPage", explode(col('connectedPage')))

df_count = df_connectedPage.groupBy('connectedPage').count()

df_result = df_count.orderBy(col('count').desc()).limit(10)
df_result = df_result.withColumn(
            'connectedPage', col('connectedPage').cast(StringType())
            ).orderBy(col('connectedPage').asc())

rows = df_result.collect()
with open(sys.argv[2], "w") as output:
    for row in rows:
        line_to_write = '%s\t%s' % (row['connectedPage'], row['count'])
        output.write(line_to_write + "\n")
#write results to output file. Foramt for each line: (line+"\n")

sc.stop()