#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import split, col, trim, udf, lit, explode, dense_rank, desc, asc, sum, rank
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

leagueIds = sc.textFile(sys.argv[2], 1)

# to have RDD.toDF function, you need to implicitly have a spark session
spark = SparkSession(sc)

# Coverting RDD to Dataframe with 1 column
fixed_rdd = lines.map(lambda x: Row(x))
league_rdd = leagueIds.map(lambda x: Row(x))

# corresponding schema
sch = StructType([StructField('inputLine', StringType(), True)])
sch2 = StructType([StructField('league_id', StringType(), True)])

# then finally, convert rdds to a DF
df = fixed_rdd.toDF(schema=sch)
df_league = league_rdd.toDF(schema=sch2)\
    .withColumn('league_id', col('league_id').cast(IntegerType()))

# Split output into dataframe with 2 column
df2 = df.withColumn('page', split(df['inputLine'], ':').getItem(0).cast(IntegerType()))\
       .withColumn ('connectedPage', split(df['inputLine'], ':').getItem(1))\
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

# Dataframe for link : number of links connected to it
df_connectedPage = df_clean.select('connectedPage').withColumn("connectedPage", explode(col('connectedPage')))
df_connectedPage = df_connectedPage.groupBy('connectedPage').count()

df_page = df_clean.select('page')\
    .withColumn('page', col('page').cast(IntegerType()))\
    .withColumnRenamed('page', 'connectedPage')\
    .withColumn('count', lit(0))\
    .distinct()

df_count = df_connectedPage.union(df_page)

df_count = df_count\
    .groupBy('connectedPage')\
    .sum('count')\
    .withColumnRenamed('sum(count)', 'count')

# Filter count to specific league only
df_result = df_league.join(df_count, df_league.league_id == df_count.connectedPage, how='left')
# handle non-existing pages
df_result = df_result.withColumn(
    'count',
    udf(lambda cp, cnt: cnt if cp else 0, IntegerType()) (col('connectedPage'), col('count'))
)

how_to_rank = Window.orderBy(asc('count'), desc('connectedPage'))
df_result = df_result\
    .withColumn('rank', dense_rank().over(how_to_rank))\
    .drop('connectedPage', 'count')

# change rank to start from 0
df_result = df_result.withColumn(
    'rank',
    udf(lambda rk: rk-1, IntegerType()) (col('rank'))
)

# sort according to whatever you want
df_result = df_result.withColumn(
    'league_id',
    col('league_id').cast(StringType())
)
df_result = df_result.orderBy('league_id')

# get it out
rows = df_result.collect()

with open(sys.argv[3], "w") as output:
    for row in rows:
        line_to_write = '%s\t%s' % (row['league_id'], row['rank'])
        output.write(line_to_write + "\n")
#write results to output file. Foramt for each line: (line+"\n")

sc.stop()
