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
		r = parse_raw(data, get_user=True)
	finally:
		return r


def parse_timestamp(data):
	''' Tries to parse timestamps with JSON formatting,
		if not, does raw text formatting
	'''
	try:
		r = parseJSON(data, get_user=False)
	except:
		r = parse_raw(data, get_user=False)
	finally:
		return r



## STREAM ANALYSIS ------------------------------------------------------------

# Connect to stream CHANGE!
sc = SparkContext("local[2]", "MyApp")
ssc = StreamingContext(sc, 10)

# from github apache/spark :: kafka_wordcount.py
# zkQuorum, topic = sys.argvs[1:]
# raw_msgs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})


# Get stream of raw messages
raw_msgs = ssc.socketTextStream("localhost", 9999)

# From raw message stream, get user stream [ <user>, <user>, ... ]
users = raw_msgs.flatMap( parse_user )

# From raw message stream, get timestamp stream [ <timestamp>, <timestamp>, ... ]
timestamps = raw_msgs.flatMap( parse_timestamp )


# Get activity counts (total and unique user)
   # using windows of 10 minutes, with 1 minute batches
message_count = users.countByWindow(60,10) # 600, 60
user_count = users.countByValueAndWindow(60,10)


message_count.pprint()
user_count.pprint()

# Save into textfile CHANGE!
# message_count.saveAsTextFiles('file:///data/activity/msgs_')
# user_count.saveAsTextFiles('file:///data/activity/users_')


# Update HBase KPI table
# TODO


# Initialize Stream
ssc.start()
ssc.awaitTermination()

