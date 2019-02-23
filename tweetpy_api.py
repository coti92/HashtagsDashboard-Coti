from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import socket
import sys
import json


# Twitter consumer key, consumer secret, access token, access secret
ACCESS_TOKEN = '741468980-hirgAI1iuJr8RyLlWS4zX86YsFVTsvnH84cNw4ND'
ACCESS_SECRET = 'FcUyRls8TBRQkloHTiWMqxzt1kboBnKXe7zDA8KEIZIjJ'
CONSUMER_KEY = 'HxTwHU4NS9ozBy5umXh5Y0VYv'
CONSUMER_SECRET = 'gnlrkkoYNUNZT2B2sshoVIvfkjvRRosI7DWcLNlZ89Ubwrkqbt'


def send_tweets_to_spark(tweet_text, tcp_connection):

    try:
        print("Tweet Text: " + tweet_text)
        print("------------------------------------------")
        tcp_connection.send(tweet_text + '\n')
        print("------------------------------------------")
    except :
        e = sys.exc_info()[0]
        print("Error: %s" % e)

# set up stream listener
class listener(StreamListener):

    def on_data(self, data):
        all_data = json.loads(data)
        # collect all desired data fields
        if 'text' in all_data:
            tweet = all_data["text"]
            send_tweets_to_spark(tweet, conn)
            return True
        else:
            return True

    def on_error(self, status):
        print(status)


auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

TCP_IP = "localhost"
TCP_PORT = 9009
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")
# create stream and filter on a searchterm
twitterStream = Stream(auth, listener())
twitterStream.filter(languages=["en"], stall_warnings=True)