import socket
import sys
import requests
import requests_oauthlib
import json


ACCESS_TOKEN = '741468980-hirgAI1iuJr8RyLlWS4zX86YsFVTsvnH84cNw4ND'
ACCESS_SECRET = 'FcUyRls8TBRQkloHTiWMqxzt1kboBnKXe7zDA8KEIZIjJ'
CONSUMER_KEY = 'HxTwHU4NS9ozBy5umXh5Y0VYv'
CONSUMER_SECRET = 'gnlrkkoYNUNZT2B2sshoVIvfkjvRRosI7DWcLNlZ89Ubwrkqbt'


auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)


def get_tweets():

    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'en'), ('locations', '-130,-20,100,50'), ('track', '#')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=auth, stream=True)
    print(query_url, response)
    return response


def send_tweets_to_spark(http_resp, tcp_connection):

    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text']
            print("Tweet Text: " + tweet_text)
            print("------------------------------------------")
            tcp_connection.send(tweet_text + '\n')
        except :
            e = sys.exc_info()[0]
            print("Error: %s" % e)


def aggregate_tags_count(new_values, total_sum):

    return sum(new_values) + (total_sum or 0)


def get_sql_context_instance(spark_context):

    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']


def process_rdd(time, rdd):

    print("----------- %s -----------" % str(time))

    try:
        # obtén el contexto spark sql singleton desde el contexto actual
        sql_context = get_sql_context_instance(rdd.context)

        # convierte el RDD a Row RDD
        row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))

        # crea un DF desde el Row RDD
        hashtags_df = sql_context.createDataFrame(row_rdd)

        # Registra el marco de data como tabla
        hashtags_df.registerTempTable("hashtags")

        # obtén los 10 mejores hashtags de la tabla utilizando SQL e imprímelos
        hashtag_counts_df = sql_context.sql(
            "select hashtag, hashtag_count from hashtags order by hashtag_count desc limit 10")
        hashtag_counts_df.show()

        # llama a este método para preparar los 10 mejores hashtags DF y envíalos
        send_df_to_dashboard(hashtag_counts_df)

    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


def send_df_to_dashboard(df):
    # extrae los hashtags del marco de data y conviértelos en una matriz
    top_tags = [str(t.hashtag) for t in df.select("hashtag").collect()]

    # extrae las cuentas del marco de data y conviértelos en una matriz
    tags_count = [p.hashtag_count for p in df.select("hashtag_count").collect()]

    # inicia y envía la data a través de la API REST
    url = 'http://localhost:5001/updateData'
    request_data = {'label': str(top_tags), 'data': str(tags_count)}
    response = requests.post(url, data=request_data)


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


##############STREAMNG##############

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

# crea una configuración spark
conf = SparkConf()
conf.setAppName("TwitterStreamApp")

# crea un contexto spark con la configuración anterior
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# crea el Contexto Streaming desde el contexto spark visto arriba con intervalo de 2 segundos
ssc = StreamingContext(sc, 2)

# establece un punto de control para permitir la recuperación de RDD
ssc.checkpoint("checkpoint_TwitterApp")

# lee data del puerto 9009
dataStream = ssc.socketTextStream("localhost",9009)

# divide cada Tweet en palabras
words = dataStream.flatMap(lambda line: line.split(" "))

# filtra las palabras para obtener solo hashtags, luego mapea cada hashtag para que sea un par de (hashtag,1)
hashtags = words.filter(lambda w: '#' in w).map(lambda x: (x, 1))

# agrega la cuenta de cada hashtag a su última cuenta
tags_totals = hashtags.updateStateByKey(aggregate_tags_count)

# procesa cada RDD generado en cada intervalo
tags_totals.foreachRDD(process_rdd)

# comienza la computación de streaming
ssc.start()

# espera que la transmisión termine
ssc.awaitTermination()