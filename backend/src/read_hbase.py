## PySpark - Activity Monitor
##    Slafka - Dec, 2015

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


import sys
import json



## STREAM ANALYSIS ------------------------------------------------------------

# Initialize stream
sc = SparkContext("local[2]", "MyApp")


# Update HBase KPI table
# host = 'localhost:2181'
host = 'slafka.c.w205-willahscott.internal'
table = 'slack_daily'


conf = {"hbase.zookeeper.quorum": host, "hbase.zookeeper.property.clientPort": port, "hbase.mapreduce.inputtable": table}
# conf = {"hbase.zookeeper.quorum": host, "hbase.mapreduce.inputtable": table}

keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"

hbase_rdd = sc.newAPIHadoopRDD(
    "org.apache.hadoop.hbase.mapreduce.TableInputFormat",
    "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
    "org.apache.hadoop.hbase.client.Result",
    keyConverter=keyConv,
    valueConverter=valueConv,
    conf=conf)

hbase_rdd = hbase_rdd.flatMapValues(lambda v: v.split("\n")).mapValues(json.loads)


print 'ZZZZZZZZZZ -  Wassaaaaa'

print hbase_rdd.count()

print 'YYY UUU UUUU UUUUUUUUUUUZZ -  Wassaaaaa'


output = hbase_rdd.collect()
for (k, v) in output:
    print((k, v))


