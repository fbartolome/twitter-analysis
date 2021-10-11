import sys

import re
from emoji import UNICODE_EMOJI
from datetime import timedelta, datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructType,
    StructField,
    TimestampType,
    ArrayType,
    LongType
)


def validate_params(args):
    if len(args) != 3:
        print(f"""
         |Usage: {args[0]} <brokers> <topics>
         |  <brokers> is a list of one or more Kafka brokers
         |  <topic> is a a kafka topic to consume from
         |
         |  {args[0]} kafka:9092 stocks
        """)
        sys.exit(1)
    pass


def create_spark_session():
    return (
        SparkSession
        .builder
        .appName("Twitter-emoji:Stream:ETL")
        .config("spark.driver.memory", "512m")
        .config("spark.executor.memory", "512m")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

emojis_list = map(lambda x: ''.join(x.split()), UNICODE_EMOJI["en"].keys())
escape_list = '|'.join(re.escape(p) for p in emojis_list)

def find_all_emo(plain_text):
    if plain_text is None:
        return None
    emo_list = re.findall(escape_list,plain_text)
    return emo_list

get_emojis = F.udf(lambda x: find_all_emo(x), ArrayType(StringType()))

def fix_date(timestamp):
    return datetime.strptime(' '.join(timestamp.split()[1:4] + [timestamp[-4:]]), '%b %d %H:%M:%S %Y') - timedelta(hours=3)

def get_hour_quarter(fixed_date):
    quarter = fixed_date.minute
    if quarter < 15:
        quarter = '00'
    elif quarter < 30:
        quarter = '15'
    elif quarter < 45:
        quarter = '30'
    else:
        quarter = '45'
    return '%s:%s' % (datetime.strftime(fixed_date, '%H'),quarter)

get_day = F.udf(lambda x: datetime.strftime(fix_date(x), '%Y-%m-%d'))
get_hour = F.udf(lambda x: get_hour_quarter(fix_date(x)))

def start_stream(args):
    validate_params(args)
    _, brokers, topic = args

    spark = create_spark_session()
    
    json = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topic)
        .load()
    )
    
    json.printSchema()

    schema = StructType([
        StructField("created_at", StringType(), False),
        StructField("user_id", LongType(), False),
        StructField("tweet_id", LongType(), False),
        StructField("tweet", StringType(), False),
    ])

    twitter_json = json.select(
        F.from_json(F.col("value").cast("string"), schema).alias("content")
    )

    twitter_json.printSchema
    tweets = twitter_json.select("content.*")

    query = (
        tweets
        .withColumn("emojis", get_emojis(F.col("tweet")))
        .withColumn("ds", get_day(F.col("created_at")))
        .withColumn("hour", get_hour(F.col("created_at")))
        .filter(F.size('emojis') > 0)
        .withColumn("emoji", F.explode("emojis"))
        .select("ds","hour","user_id","tweet_id","emoji")
        .distinct()
        .writeStream
        .format("parquet")
        .partitionBy("ds", "hour")
        .option("startingOffsets", "earliest")
        .option("checkpointLocation", "/dataset/checkpoint")
        .option("path", "/dataset/streaming.parquet")
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()




if __name__ == "__main__":
    start_stream(sys.argv)
