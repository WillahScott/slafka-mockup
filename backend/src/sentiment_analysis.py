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

def parseJSON(js):
	''' Parse JSON, outputs text (or user, text)
	'''
	data = json.loads(js)
	header = data['header']

	# return {'username': header['username'], 'text': header['text']}
	return header['text']


def parse_raw(st):
	''' Parse raw string, outputs text (or user, text)
	'''
	headers = st.split(', ')

	# user_raw = [ d for d in headers if "user_name" in d]
	# user = user_raw[0].split('=')[1]

	text_raw = [ d for d in headers if "text=" in d]
	text = text_raw[0].split('=')[1] if text_raw else ''

	# return {'username': user, 'text': text}
	return text


def parse(data):
	''' Tries to parse with JSON formatting,
		if not, does raw text formatting
	'''
	try:
		r = parseJSON(data)
	except:
		r = parse_raw(data)
	finally:
		return r


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
	pk.dump( open('corpus/updated_corpus.pk', 'wb') )



## INITIALIZATION -------------------------------------------------------------

# Read sentiment dictionary
score_corpus = {} # initialize an empty dictionary	

with open('corpus/AFINN-sentiment.txt') as f:
	s_file = f.readlines()

for line in s_file:
	term, score  = line.split("\t")  # The file is tab-delimited "\t"
	score_corpus[term] = int(score)  # Convert the score to an integer.

# Initialize new corpus (contains total score and total count of messages)
new_corpus = load_updated_corpus()

# Registers saving the updated corpus into the corpus/ directory on program exit
atexit.register( save_updated_corpus )



## STREAM ANALYSIS ------------------------------------------------------------

# Connect to stream CHANGE!
sc = SparkContext("local[2]", "MyApp")
ssc = StreamingContext(sc, 10)

# from github apache/spark :: kafka_wordcount.py
# zkQuorum, topic = sys.argv[1:]
# raw_msgs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})


# Parse stream, returns -> [ <message>, <message>, ... ]
raw_msgs = ssc.socketTextStream("localhost", 9999)
messages = raw_msgs.flatMap( parse )

# Apply sentiment score
scores = messages.flatMap( get_score )
# scored_msgs = messages.flatMap( get_score_update )

scored_msgs = messages.join(scores)

# Save into textfile CHANGE!
scored_msgs.saveAsTextFiles('file:///data/scored_messages/sc_msg_')


# Initialize Stream
ssc.start()
ssc.awaitTermination()



