import requests
import json

token="xoxp-11861152913-11857269303-11854775491-dde373b441"
channel="C0BR76PDM"
url1 = "https://slack.com/api/channels.history?token="+token+"&channel="+channel+"&pretty=1"

print url1
response = requests.request("GET", url1)

parsed_json = json.loads(response.text)

#print(parsed_json['messages'])
f = open('history_load.dat', 'wt')


for message in parsed_json['messages']:
    f.write(message['type']+'\t'+message['user']+'\t'+message['text']+'\t'+message['ts']+'\n')

while(parsed_json['has_more']):
    latestTimestamp=parsed_json['messages'][len(parsed_json['messages'])-1]['ts']
    url2 = "https://slack.com/api/channels.history?token="+token+"&channel="+channel+"&latest="+latestTimestamp+"&pretty=1"
    print(url2)
    response = requests.request("GET", url2)
    parsed_json = json.loads(response.text)
    for message in parsed_json['messages']:
        f.write(message['type'].encode('ascii', 'ignore')+'\t'+message['user'].encode('ascii', 'ignore')+'\t'+message['text'].encode('ascii', 'ignore')+'\t'+message['ts']+'\n')

f.close()
#print(response.text)
