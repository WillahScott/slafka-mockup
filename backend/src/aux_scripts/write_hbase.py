## PySpark - Activity Monitor
##    Slafka - Dec, 2015

# USAGE:
# spark-submit --driver-class-path /opt/cloudera/parcels/CDH-5.5.0-1.cdh5.5.0.p0.8/lib/spark/lib/spark-examples.jar write_hbase.py

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


import sys
import json



## STREAM ANALYSIS ------------------------------------------------------------

# Initialize stream
sc = SparkContext("local[2]", "MyApp")


# Update HBase KPI table
host = 'localhost:2181'
table = 'slafka_daily'

row = '2015-12-17'
family = 'tsa'
q1 = 'uniqueUsers' 
q2 = 'totalMsgs'
q3 = 'totalSentiment'


# Update the HBase table
conf = {"hbase.zookeeper.quorum": host,
        "hbase.mapred.outputtable": table,
        "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
        "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
        "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"
        }
keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"


row1 = (row, [row, family, q1, '33'])
row2 = (row, [row, family, q2, '111'])

sc.parallelize([ row1, row2 ]).saveAsNewAPIHadoopDataset(
               conf=conf,
               keyConverter=keyConv,
               valueConverter=valueConv
               )


