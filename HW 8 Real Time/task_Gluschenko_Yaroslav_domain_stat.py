# !/usr/bin/env python3

import argparse

from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import split
from pyspark.sql import SQLContext


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka-brokers", required=True)
    parser.add_argument("--topic-name", required=True)
    parser.add_argument("--starting-offsets", default='latest')

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--processing-time", default='0 seconds')
    group.add_argument("--once", action='store_true')
    args = parser.parse_args()

    if args.once:
        args.processing_time = None
    else:
        args.once = None

    return args

def main(args):
    page_views = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", args.kafka_brokers) \
        .option('subscribe', args.topic_name) \
        .option("startingOffsets", args.starting_offsets) \
        .load()

    df = page_views.selectExpr("cast(value as string)")
    split_col = split(df['value'], '\t')

    df = df.withColumn('uid', split_col.getItem(1))
    df = df.withColumn('url', split_col.getItem(2))
    df.createTempView("tmp")

    df2 = spark.sql("""
        SELECT
            parse_url(url, 'HOST') as domain,
            COUNT(uid) as view,
            approx_count_distinct(uid) as unique
        FROM
            tmp
        GROUP BY
            domain
        ORDER BY
            view DESC
        LIMIT 10
    """)
    spark.catalog.dropTempView("tmp")

    query = df2 \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(once=args.once, processingTime=args.processing_time) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    parsered_args = parse_arguments()

    # create spark context
    sc = SparkContext()

    spark = SparkSession(sc)
    spark.sparkContext.setLogLevel("WARN")

    sqlContext = SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "4")

    main(parsered_args)

    sc.stop()
