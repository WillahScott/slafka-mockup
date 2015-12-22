## PySpark - Activity Monitor
##    Slafka - Dec, 2015

# USAGE:
# spark-submit --driver-class-path /opt/cloudera/parcels/CDH-5.5.0-1.cdh5.5.0.p0.8/lib/spark/lib/spark-examples.jar read_hbase.py

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
table = 'slafka_daily'
port = '2181'


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


output = hbase_rdd.collect()

for (k, v) in output:
	if k == date:
	    print((k, v))


