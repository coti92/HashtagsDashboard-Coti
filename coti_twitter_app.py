import socket
import sys
import tweepy

consumer_key = 'HxTwHU4NS9ozBy5umXh5Y0VYv'
consumer_secret = 'gnlrkkoYNUNZT2B2sshoVIvfkjvRRosI7DWcLNlZ89Ubwrkqbt'

access_token = '741468980-hirgAI1iuJr8RyLlWS4zX86YsFVTsvnH84cNw4ND'
access_token_secret = 'FcUyRls8TBRQkloHTiWMqxzt1kboBnKXe7zDA8KEIZIjJ'

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

def get_tweets():

    public_tweets = api.search('Suicide')
    return public_tweets


def send_tweets_to_spark(public_tweets, tcp_connection):

    for tweet in public_tweets:
        try:
            tweet_text = tweet.text
            print("Tweet Text: " + tweet_text)
            print("------------------------------------------")
            tcp_connection.send(tweet_text + '\n')
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
