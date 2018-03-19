from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import sys

def identify_sentiment(v):
    try:
        if v < 0:
            return 'NEGATIVE'
        elif v == 0:
            return 'NEUTRAL'
        else:
            return 'POSITIVE'
    except TypeError:
        return 'NEUTRAL'

if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: twitter_sentiment.py <kafka_bootstrap_server:port> <topic>", file=sys.stderr)
        exit(-1)

    kafka_bootstrap_server, topic = sys.argv[1:]

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("location", StringType(), True),
        StructField("text", StringType(), True),
        StructField("senti_val", DoubleType(), True)
    ])

    spark = SparkSession \
        .builder \
        .appName("Project14") \
        .getOrCreate()

    # only show ERROR
    spark.sparkContext.setLogLevel("ERROR")

    # create dataframe to read from kafka
    kfk_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_server) \
        .option("subscribe", topic) \
        .load()

    # convert data from kafka to dataframe
    kfk_df_string = kfk_df.selectExpr("CAST(value AS STRING)")

    tweets_table = kfk_df_string \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    neg_sent = tweets_table.select("senti_val")

    udf_sent_to_status = udf(identify_sentiment, StringType())

    # map sentiment number to string
    new_df = neg_sent.withColumn("status", udf_sent_to_status("senti_val"))

    output = new_df.groupBy("status").count()

    # print to console for debugging
    # query = output \
    #     .writeStream \
    #     .outputMode("complete") \
    #     .format("console") \
    #     .start()

    query = output \
        .selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .outputMode("complete") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_server) \
        .option("topic", "sentiment") \
        .option("checkpointLocation", "/home/ec2-user/checkpoint") \
        .start()

    query.awaitTermination()
