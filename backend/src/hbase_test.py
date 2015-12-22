## PySpark - Activity Monitor
##    Slafka - Dec, 2015

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from pyspark.streaming.kafka import KafkaUtils

import sys
import json



## STREAM ANALYSIS ------------------------------------------------------------

# Initialize stream
sc = SparkContext("local[2]", "MyApp")


# Update HBase KPI table
host = 'localhost'
table = 'slack_daily'

row = '2015-12-20'
family = 'tsa'
q1 = 'uniqueUsers' 
q2 = 'totalMsgs'
q3 = 'totalSentiment'

# # Read from HBase table
# conf = {"hbase.zookeeper.quorum": host,
#         "zookeeper.znode.parent": sys.argv[3],  # column_family ??
#         "hbase.mapreduce.inputtable": table}
# keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
# valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"

# hbase_rdd = sc.newAPIHadoopRDD(
#     "org.apache.hadoop.hbase.mapreduce.TableInputFormat",
#     "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
#     "org.apache.hadoop.hbase.client.Result",
#     keyConverter=keyConv,
#     valueConverter=valueConv,
#     conf=conf)



# Update the HBase table
conf = {"hbase.zookeeper.quorum": host,
        "hbase.mapred.outputtable": table,
        "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
        "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
        "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"
        }
keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"

sc.parallelize([ ('2015-12-21', ['2015-12-21', family, q1, 3]), ('2015-12-21', ['2015-12-21', family, q2, 10]) ]).saveAsNewAPIHadoopDataset(
               conf=conf,
               keyConverter=keyConv,
               valueConverter=valueConv
               )


# Initialize Stream
ssc.start()
ssc.awaitTermination()

