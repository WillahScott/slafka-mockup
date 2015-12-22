## PySpark - Activity Monitor
##    Slafka - Dec, 2015

# USAGE:
# spark-submit --driver-class-path /opt/cloudera/parcels/CDH-5.5.0-1.cdh5.5.0.p0.8/lib/spark/lib/spark-examples.jar activity_monitor.py

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


def parse_date(stmp):
	''' From a timestamp returns the date with HBase row format
	'''
	print '--------------------------- PARSING DATES --------------------------------------'
	_date = datetime.fromtimestamp(float(stmp))
	date = '-'.join(map(str, [_date.year, _date.month, _date.day]))
	return date



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
ssc.checkpoint("file:///apps/new-slafka/slafka-mockup/backend/data/activity/checkpointingte")

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
_time_latest = times.reduce( max ).map( parse_date )

_counts = _message_count.join(_act_user_count)
final_stream = _time_latest.join(_counts)

final_stream.pprint()


# Collect results
# message_count = _message_count.collect()
# act_user_count = _act_user_count.collect()
# time_latest = _time_latest.collect()

# print "MESSAGES:", message_count, "USERS:", act_user_count, "TIME:", time_latest


# Print for debug
# message_count.pprint()
# act_user_count.pprint()
# time_latest.pprint()


# # Convert timestamp into date
# _date = datetime.fromtimestamp(float(time_latest))
# date_str = '-'.join(map(str, [_date.year, _date.month, _date.day]))
# print "DATE", date_str

date_str = time_latest
print "--> DATE", date_str, type(date_str)





# Initialize Stream
ssc.start()
ssc.awaitTermination()

