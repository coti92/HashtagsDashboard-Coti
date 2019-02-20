import socket
import sys
import requests
import requests_oauthlib
import json
from textblob import TextBlob


ACCESS_TOKEN = '741468980-hirgAI1iuJr8RyLlWS4zX86YsFVTsvnH84cNw4ND'
ACCESS_SECRET = 'FcUyRls8TBRQkloHTiWMqxzt1kboBnKXe7zDA8KEIZIjJ'
CONSUMER_KEY = 'HxTwHU4NS9ozBy5umXh5Y0VYv'
CONSUMER_SECRET = 'gnlrkkoYNUNZT2B2sshoVIvfkjvRRosI7DWcLNlZ89Ubwrkqbt'


auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)


def get_tweets():

    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    # query_data = [('language', 'en'), ('locations', '-130,-20,100,50'), ('track', '#')]
    # query_data = [('language', 'es'), ('locations','1.883205,41.223872,2.348228,41.504076'), ('track', 'suicidio')]
    query_data = [('language', 'en'), ('locations', '-166.7,9.1,-49.2,72.2'), ('track', '#,suicide,death,mental illness,kamikaze,depression,kill,homicide,seppuku,alcoholism,bipolar disorder,schizophrenia,suicidal ideation,felo-de-se,killer,killing,murder,assassination,major depressive disorder,substance abuse,bullying,assisted suicide,self-harm,terrorist,sin,euthanasia,self-annihilation,self-destruction,slayer,borderline personality disorder,mental disorder,deadly,bombing,attack,fatal,bomb,revenge,risk factor')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=auth, stream=True)
    print(query_url, response)
    return response


def send_tweets_to_spark(http_resp, tcp_connection):

    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            #print(json.dumps(full_tweet, indent=4, sort_keys=True))
            tweet_text = full_tweet['text']
            analysis = TextBlob(tweet_text)
            print(analysis.sentiment)
            print("Tweet Text: " + tweet_text)
            print("------------------------------------------")
            if (analysis.sentiment.polarity < 0):
                tcp_connection.send(tweet_text + '\n')
                print("------------------------------------------")
        except :
            e = sys.exc_info()[0]
            print("Error: %s" % e)

TCP_IP = "localhost"
TCP_PORT = 9009
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")
resp = get_tweets()
send_tweets_to_spark(resp, conn)
