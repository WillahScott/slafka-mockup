
# PySpark Commands


## Initialize PySpark with n cores
`pyspark --master local[n]`


## PySpark
```python
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

ssc = StreamingContext(sc, 1)

# moving files into a folder
lines = ssc.textFileStream("file:///root/lab12/datastreams")

# listening to a socket
lines = ssc.socketTextStream("localhost", 9999)

uclines = lines.map( lambda w: w.upper() )
uclines.pprint()

ssc.start()

ssc.stop()
```

## JSON Loader
```
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json

ssc = StreamingContext(sc, 10)
lines = ssc.textFileStream("file:///root/lab12/datastreams")
slines = lines.flatMap( lambda x: [ j['venue'] for j in json.loads('['+x+']') if 'venue' in j ] )
cnt = slines.count()
cnt.pprint()
slines.saveAsTextFiles('file:///root/lab12/datastreams/venues_')

ssc.start()
ssc.stop()
```

## JSON Listener
```python
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json

ssc = StreamingContext(sc, 10)
lines = ssc.socketTextStream("localhost", 9999)
jslines = lines.flatMap( lambda x: [ j['venue'] for j in json.loads('['+x+']') if 'venue' in j ] )
lcnt = lines.count()
lcnt.pprint()
c = jslines.count()
c.pprint()
jslines.pprint()

ssc.start()
ssc.stop()
```


## Venue Counter
```python
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json

sc = SparkContext("local[2]", "MyApp")
ssc = StreamingContext(sc, 10)
lines = ssc.socketTextStream("localhost", 9999)
jslines = lines.flatMap( lambda x: [ j['venue'] for j in json.loads('['+x+']') if 'venue' in j ] )
lcnt = lines.count()
lcnt.pprint()
c = jslines.count()
c.pprint()
jslines.pprint()

ssc.start()
ssc.awaitTermination()
```


## Venue Counter with WINDONWING
```python
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json

sc = SparkContext("local[2]", "MyApp")
ssc = StreamingContext(sc, 10)
ssc.checkpoint("file:///root/lab12/checkpointing")

lines = ssc.socketTextStream("localhost", 9999)
wlcnt = lines.countByWindow(30,10)

jslines = lines.flatMap( lambda x: [ j['venue'] for j in json.loads('['+x+']') if 'venue' in j ] )
lcnt = lines.count()

wlcnt.pprint()
lcnt.pprint()

c = jslines.count()
c.pprint()
jslines.pprint()

ssc.start()
ssc.awaitTermination()
```




