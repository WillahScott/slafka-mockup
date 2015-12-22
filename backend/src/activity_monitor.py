## PySpark - Activity Monitor
##    Slafka - Dec, 2015

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from pyspark.streaming.kafka import KafkaUtils

import sys
import json
from datetime import datetime


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
_message_count = users.count() # 600, 60
_act_user_count = users.countByValue()
_time_latest = times.reduce( max )


print "MESSAGES:", _message_count, "USERS:", _act_user_count, "TIME:", _time_latest

# Collect results
# message_count = _message_count.collect()
# act_user_count = _act_user_count.collect()
# time_latest = _time_latest.collect()

# print "MESSAGES:", message_count, "USERS:", act_user_count, "TIME:", time_latest


# Print for debug
# message_count.pprint()
# act_user_count.pprint()
# time_latest.pprint()


# Convert timestamp into date
_date = datetime.fromtimestamp(float(time_latest))
date_str = '-'.join(map(str, [_date.year, _date.month, _date.day]))
print "DATE", date_str


## HBASE INTERACTION ----------------------------------------------------------

# Global host-table configuration
host = 'localhost:2181'
table = 'slafka_daily'


# Configuration for HBase read
conf_read = {"hbase.zookeeper.quorum": host, "hbase.mapreduce.inputtable": table}
keyConv_read = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
valueConv_read = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"


# Read actual table
hbase_rdd = sc.newAPIHadoopRDD(
    "org.apache.hadoop.hbase.mapreduce.TableInputFormat",
    "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
    "org.apache.hadoop.hbase.client.Result",
    keyConverter=keyConv_read,
    valueConverter=valueConv_read,
    conf=conf_read)
hbase_rdd = hbase_rdd.flatMapValues(lambda v: v.split("\n")).mapValues(json.loads)
output = hbase_rdd.collect()


# Record values of row corresponding to date
update_values = {}
for (d, v) in output:
	if d == date_str:
		update_values[v['qualifier']] = v['value']

# If necessary update values
if len(update_values) > 0:
	message_count = str( int(update_values['totalMsgs']) + int(message_count) )
	act_user_count = str( int(update_values['uniqueUsers']) + int(act_user_count) )


# Write into table
family = 'tsa'
q1 = 'uniqueUsers' 
q2 = 'totalMsgs'
q3 = 'totalSentiment'


# Configuration for HBase write
conf_write = {"hbase.zookeeper.quorum": host,
              "hbase.mapred.outputtable": table,
              "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
              "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
              "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"
              }
keyConv_write = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
valueConv_write = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"


# Update the HBase table
row1 = ( date_str, [date_str, family, 'totalMsgs', message_count] )
row2 = ( date_str, [date_str, family, 'uniqueUsers', act_user_count] )
row3 = ( date_str, [date_str, family, 'latestTimestamp', time_latest] )

sc.parallelize([ row1, row2, row3 ]).saveAsNewAPIHadoopDataset(
               keyConverter=keyConv_write,
               valueConverter=valueConv_write,
               conf=conf_write
               )




# Initialize Stream
ssc.start()
ssc.awaitTermination()

