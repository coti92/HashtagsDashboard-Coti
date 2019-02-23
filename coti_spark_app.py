from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests


def get_sql_context_instance(spark_context):

    if ('sqlContextSingletonInstance' not in globals()):

        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)

    return globals()['sqlContextSingletonInstance']


def process_rdd(time, rdd):

    print("----------- %s -----------" % str(time))

    try:
        # obten el contexto spark sql sngleton desde el contexto actual
        sql_context = get_sql_context_instance(rdd.context)

        # convierte el RDD a Row RDD
        row_rdd = rdd.map(lambda w: Row(tweet=w[0], tweet_count=w[1]))

        # crea un DF desde el Row RDD
        tweet_df = sql_context.createDataFrame(row_rdd)

        # Registra el marco de data como tabla
        tweet_df.registerTempTable("tweet")

        # obten los 10 mejores hashtags de la tabla utilizando sqL e imprimelos
        tweet_counts_df = sql_context.sql( "select tweet , tweet_count from tweet where (tweet_count > 2 and tweet <> '' and tweet is not null) order by tweet_count desc limit 10" )
        tweet_counts_df.show()

        # llama a este metodo para preparar los 10 mejores hashtag DF y envialos
        send_df_to_dashboard(tweet_counts_df)

    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


def aggregate_tags_count(new_values, total_sum):


    return sum(new_values) + (total_sum or 0)


def send_df_to_dashboard(df):
    # extrae los hashtags del marco de data y conviertelos en una matriz
    top_tags = [str(t.tweet) for t in df.select("tweet").collect()]

    # extrae las cuentas del marco de data y conviertelos en una matriz
    tags_count = [p.tweet_count for p in df.select("tweet_count").collect()]

    # inicia y envia la data a traves de la API REST
    url = 'http://localhost:5002/updateData'
    request_data = {'label': str(top_tags), 'data': str(tags_count)}
    response = requests.post(url, data=request_data)



# crea una configuracion spark
config = SparkConf()
config.setAppName("TwitterStreamApp")

# crea un contexto spark con la configuracion anterior
sc = SparkContext(conf=config)
sc.setLogLevel("ERROR")

# crea el Contexto Streaming desde el contexto spark visto arriba con intervalo de 2 segundos
ssc = StreamingContext(sc, 2)

# establece un punto de control para emitir la recuperacion de RDD
ssc.checkpoint("checkpoint_TwitterApp")

# lee data del puerto 9009
dataStream = ssc.socketTextStream("localhost",9008)

# filtra las palabras para obtener solo hashtags, luego mapea cada hashtag para que sea un par de (hashtag,1)
tweet = dataStream.map(lambda x: (x, 1))

# agrega la cuenta de cada hashtag a su ultima cuenta
tags_totals = tweet.updateStateByKey(aggregate_tags_count)

# procesa cada RDD generado en cada intervalo
tags_totals.foreachRDD(process_rdd)

# comienza el streaming
ssc.start()

# espera que la transmision termine
ssc.awaitTermination()
