#!/usr/bin/env python

'''Exectuion Command: spark-submit TitleCountSpark.py stopwords.txt delimiters.txt dataset/titles/ dataset/output'''

import re
import sys
from urllib.parse import quote

from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import udf, col, explode
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

stopWordsPath = sys.argv[1]
delimitersPath = sys.argv[2]
dataPath = sys.argv[3]

with open(stopWordsPath) as f:
    stop_word_content = f.readlines()
    stop_word_list = [word.lower().strip() for word in stop_word_content]

# Contruct regular expression to split word later
with open(delimitersPath) as f:
    delimiters = f.read().strip()
sepr_list = [" "]
for c in delimiters:
    sepr_list.append(c)
reg_exp = '|'.join(map(re.escape, sepr_list))


def tokenize_title(line: str) -> [str]:
    # Split using delimiters:
    words = re.split(reg_exp, line.strip())
    # Change all token to lower case
    words = [w.lower().strip() for w in words]
    # Remove word from Stopword
    words = list(filter(lambda w: w not in stop_word_list, words))
    # Remove empty word:
    words = [w for w in words if w != ""]
    # now we have list of clean words :)
    return words


conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf = conf)

# to have RDD.toDF function, you need to implicitly have a spark session
# see explanation https://stackoverflow.com/questions/32788387/pipelinedrdd-object-has-no-attribute-todf-in-pyspark
spark = SparkSession(sc)

# read raw RDD of lines of text
lines = sc.textFile(dataPath, 1)
# change each raw line to a formal Row type - this is required to be able to convert the RDD to a dataframe (DF)
#fixed_rdd = lines.map(lambda x: Row(quote(x)))
fixed_rdd = lines.map(lambda x: Row(x))

# corresponding schema
sch = StructType([StructField('title', StringType(), True)])
# then finally, convert fixed_rdd to a DF
df = fixed_rdd.toDF(schema=sch)
"""
For understanding, this is content of the first 10 lines
>>> df.show(10, truncate=False)
+-------------------------------------------------------------------------------+
|title                                                                          |
+-------------------------------------------------------------------------------+
|Chronic_spasmodic_dysphonia                                                    |
|Mohamed_Abdu                                                                   |
|Quirites                                                                       |
|Sugar_Me                                                                       |
|KBTA                                                                           |
|Trachycephalus_lepidus                                                         |
|Choi_Gil-Soo                                                                   |
|Bench_hook                                                                     |
|%D8%AA%D9%84%D9%81%D8%B2%D9%8A%D9%88%D9%86_%D8%AC%D8%B2%D8%A7%D8%A6%D8%B1%D9%8A|
|Eruca                                                                          |
+-------------------------------------------------------------------------------+
only showing top 10 rows
"""

# From here onward, it's all dataframe operation, which is quite equivalent to SQL

# tokenize each line to an array of words
df = df.withColumn(
    'words',
    udf(tokenize_title, ArrayType(StringType())) (col('title'))
)

# explode each array of words to words
df = df.withColumn('word', explode(col('words')))

# group by word and count occurrences
total_res = df.select('word').groupBy('word').count()
# sort: DESC order of count, ASC order of word
res = total_res.orderBy(col('count').desc(), col('word').asc()).limit(10)

# for the result, sort by ASC order of word
res = res.orderBy(col('word').asc())

# select top 10 words with highest count, output to file
rows = res.collect() # this becomes an array of rows
with open(sys.argv[4], "w") as fw:
    for row in rows:
        line_to_write = '%s\t%s' % (row['word'], row['count'])
        fw.write(line_to_write + "\n")

