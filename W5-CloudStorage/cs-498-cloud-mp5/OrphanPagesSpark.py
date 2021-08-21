#!/usr/bin/env python
import sys
from _operator import add

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, trim, udf, lit, explode
from pyspark.sql.types import StructType, StructField, StringType, Row, ArrayType, IntegerType

conf = SparkConf().setMaster("local").setAppName("OrphanPages")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf = conf)


def process(line):
    scr, dests = line.split(': ')
    dests = dests.split(' ')
    d = [(dest,1) for dest in dests]
    d.append((scr,0))
    return d


lines = sc.textFile(sys.argv[1], 1)
ids = lines.flatMap(process).reduceByKey(add).filter(lambda x: x[1] == 0)\
        .map(lambda  x: x[0]).collect()

output =  open(sys.argv[2], "w")
for id in sorted(ids):
    output.write(id + "\n")
output.close()

sc.stop()