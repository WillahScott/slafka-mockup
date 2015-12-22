## PySpark - Activity Monitor
##    Slafka - Dec, 2015

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from pyspark.streaming.kafka import KafkaUtils

import sys
import json


## FUNCTIONS ------------------------------------------------------------------

def parseJSON(js, get_user=True):
	''' Parse JSON, outputs user (or user, timestamp)
	'''
	data = json.loads(js)
	header = data['header']

	if get_user:
		return header['username']

	else:
		return float(header['timestamp']) 


def parse_raw(st, get_user=True):
	''' Parse raw string, outputs user (or user, timestamp)
	'''
	headers = st.split(', ')

	if get_user:
		user_raw = [ d for d in headers if "user_name" in d]
		user = user_raw[0].split('=')[1]

		return user

	else:
		time_raw = [ d for d in headers if "timestamp=" in d]
		time = float(time_raw[0].split('=')[1]) if text_raw else 0.0

		return time


def parse_user(data):
	''' Tries to parse usernames with JSON formatting,
		if not, does raw text formatting
	'''
	try:
		r = parseJSON(data, get_user=True)
	except:
		r = '1234567890'
	finally:
		return r


def parse_timestamp(data):
	''' Tries to parse timestamps with JSON formatting,
		if not, does raw text formatting
	'''
	try:
		r = parseJSON(data, get_user=False)
	except:
		r = data
	finally:
		return r


def debug(data):
	return "\n --- \n" + data + "\n --- \n"


## STREAM ANALYSIS ------------------------------------------------------------

# Initialize stream
sc = SparkContext("local[2]", "MyApp")
ssc = StreamingContext(sc, 10)
ssc.checkpoint("file:///apps/slafka/slafka-mockup/backend/data/activity/checkpointingte")

# Get stream of raw messages from Kafka
   # from github apache/spark :: kafka_wordcount.py
zkQuorum = 'localhost:2181'
topic = 'slafka' 
raw_msgs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})


# From raw message stream, get user stream [ <user>, <user>, ... ]
users = raw_msgs.flatMap( debug )


# Get activity counts (total and unique user)
   # using windows of 10 minutes, with 1 minute batches
message_count = users.count() # 600, 60


# Debug
users.pprint()
message_count.pprint()


# Update HBase KPI table
host = ''
table = 'slack_daily'


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



# # Update the HBase table
# conf = {"hbase.zookeeper.quorum": host,
#         "hbase.mapred.outputtable": table,
#         "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
#         "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
#         "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"
#         }
# keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
# valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"

# sc.parallelize([sys.argv[3:]]).map(lambda x: (x[0], x)).saveAsNewAPIHadoopDataset(
#                conf=conf,
#                keyConverter=keyConv,
#                valueConverter=valueConv
#                )


# Initialize Stream
ssc.start()
ssc.awaitTermination()

