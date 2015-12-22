## PySpark - Sentiment Analysis
##    Slafka - Dec, 2015

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from pyspark.streaming.kafka import KafkaUtils

import sys
import atexit
import cPickle as pk
import re
import json


# Compiled regex for filtering the words
pattern = re.compile('[^a-zA-Z]+')


## FUNCTIONS ------------------------------------------------------------------

def get_score(msg):
	''' Scores a given message.
	'''
	score = 0 # initialize score to 0

	# Reads the message and splits into words
	words = msg["text"].split()

	for _w in words:
		# Converts unicode to lower-case string
		_w2 = _w.encode('utf-8').lower()

		# Strip basic symbols (except for : -> emojis)
		w = pattern.sub('', _w2)

		# Searches for the word in the score dictionary
		if w in score_corpus:
			score += score_corpus[w]

	return score # Return final message score


def get_score_update(msg):
	''' Scores a given message.
		If a given term is not in sentiment corpus adds it to
		new corpus with mean score of message.
	'''
	# Reads the message and splits into words
	words = msg["text"].split()

	# Get original (tweet score)
	score = get_score(msg)

	for _w in words:
		# Converts unicode to lower-case string
		w = _w.encode('utf-8').lower()

		# Searches for the word in the original dictionary
		if w not in score_corpus:
			# Searches for the word in the new dictionary
			if w in new_corpus:
				_sc, _cnt = new_dic[w]
			else:
				_sc, _cnt = 0,0
			
			new_corpus[w] = (score + _sc, 1 + _cnt)

	return score


def process_message(raw_msg):
	''' Parses raw messages and outputs:
	        ( <timestamp>, ( <message>, <user_name>, <sentiment_score> ) )
	'''
	# Data incoming as second term of tuple
	js = raw_msg[1]
	data = json.loads(js)

	# Parses base data
	message = data['text']
	user = data['user_name']
	timestamp = data['timestamp']

	# Get score for message
	score = get_score(message)
	# score = get_score_update(message)

	# Return parsed message
	return ( timestamp, ( message, user, score ) )



def load_updated_corpus():
	''' If exists, loads the updated corpus, else returns empyt dictionary
	'''
	try:
		f = open('corpus/updated_corpus.pk', 'rb')
	
	except IOError:
		updated_corpus = {}

	else:
		updated_corpus = pk.load(f)

	finally:
		return updated_corpus


def save_updated_corpus():
	''' Pickles the newly generated corpus into ext_corpus
	'''
	pk.dump( new_corpus, open('corpus/updated_corpus.pk', 'wb') )


def write_hbase(data):
	''' Updates the HBase table with given:
	        data = ( <timestamp>, ( <message>, <user_name>, <sentiment_score> ) )
	'''

	# # Parse data
	# date_str = data[0]
	# message = data[1][0]
	# user = data[1][1]
	# score = data[1][2]
	date_str = '1450713154.000028'
	message = 'Hello World, this is sad Will! :cry:'
	user = 'will.monge'
	score = '-4'


	# Write into the HBase table
	row1 = ( date_str, [date_str, family, 'message', message_count] )
	row2 = ( date_str, [date_str, family, 'user', act_user_count] )
	row3 = ( date_str, [date_str, family, 'score', time_latest] )

	print "===========> WRITING HBASE"
	print "===> Values to write (I/III):", row1
	print "===> Values to write (II/III):", row2
	print "===> Values to write (IIII/III):", row3


	sc.parallelize([ row1, row2, row3 ]).saveAsNewAPIHadoopDataset(
	               keyConverter=keyConv_write,
	               valueConverter=valueConv_write,
	               conf=conf_write
	               )

	return True




## SENTIMENT ANALYS INITIALIZATION --------------------------------------------

# Read sentiment dictionary
score_corpus = {} # initialize an empty dictionary	

with open('corpus/AFINN-extended.txt') as f:
	s_file = f.readlines()
f
for line in s_file:
	term, score  = line.split("\t")  # The file is tab-delimited "\t"
	score_corpus[term] = int(score)  # Convert the score to an integer.

# Initialize new corpus (contains total score and total count of messages)
new_corpus = load_updated_corpus()

# Registers saving the updated corpus into the corpus/ directory on program exit
atexit.register( save_updated_corpus )



## HBASE CONFIGURATION --------------------------------------------------------

# Global host-table configuration
host = 'localhost:2181'
table = 'slafka_daily'


# Configuration for HBase write
family = 'tsa'
conf_write = {"hbase.zookeeper.quorum": host,
              "hbase.mapred.outputtable": table,
              "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
              "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
              "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"
              }
keyConv_write = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
valueConv_write = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"



## STREAM ANALYSIS ------------------------------------------------------------

# Connect to stream CHANGE!
sc = SparkContext("local[2]", "MyApp")
ssc = StreamingContext(sc, 10)

# from github apache/spark :: kafka_wordcount.py
# zkQuorum, topic = sys.argv[1:]
# raw_msgs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})


# Parse stream, returns -> [ <message>, <message>, ... ]
raw_msgs = ssc.socketTextStream("localhost", 9999)
sc_messages = raw_msgs.flatMap( process_message )

# Debug
sc_messages.pprint()


# Update HBase with each entry
hbase_updates = sc_messages.flatMap( write_hbase )


# Initialize Stream
ssc.start()
ssc.awaitTermination()



