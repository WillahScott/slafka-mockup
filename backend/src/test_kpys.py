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

	if get_user:
		return data['user_name']

	else:
		return float(data['timestamp']) 


def parse_raw(st, get_user=True):
	''' Parse raw string, outputs user (or user, timestamp)
	'''
	data = st.split('\",\"')

	if get_user:
		user_raw = [ d for d in data if "user_name" in d]
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
	# Data incoming as second term of tuple
	try:
		r = parseJSON(data[1], get_user=True)
	except:
		r = parse_raw(data[1], get_user=True)
	finally:
		return r


def parse_timestamp(data):
	''' Tries to parse timestamps with JSON formatting,
		if not, does raw text formatting
	'''
	# Data incoming as second term of tuple
	try:
		r = parseJSON(data[1], get_user=False)
	except:
		r = parse_raw(data[1], get_user=False)
	finally:
		return r


# def get_users(data):
# 	''' Parse JSON, outputs user (or user, timestamp)
# 	'''
# 	data = json.loads(data[1])
# 	return data['user_name']

# def get_timestamps(data):
# 	data = json.loads(data[1])
# 	return data['timestamp']



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
users = raw_msgs.map( parse_user )
times = raw_msgs.map( parse_timestamp )


# Get activity counts (total and unique user)
   # using windows of 10 minutes, with 1 minute batches
message_count = users.countByWindow(10, 5) # 600, 60
act_user_count = users.countByValueAndWindow(10, 5)
time_latest = times.reduceByWindow( max )


# Print for debug
message_count.pprint()
act_user_count.pprint()
time_latest.pprint()



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

