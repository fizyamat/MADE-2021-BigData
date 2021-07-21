# /usr/bin/env python3

from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import split, trim, col, regexp_replace, regexp_extract, substring
from pyspark.sql.types import IntegerType

import sys

PATH_TO_DATA = 'hdfs:///data/movielens/movies.csv'
SEPARATOR = ','


keyspace = sys.argv[1]

# create spark context
sc = SparkContext()
spark = SparkSession(sc)

# read dataset to spark
df = spark \
    .read \
    .format('csv') \
    .load(PATH_TO_DATA, sep=SEPARATOR, header=True)

df2 = df \
    .filter(col('genres') != '(no genres listed)') \
    .select(
        df.movieId.cast(IntegerType()).alias('movieid'),
        trim(regexp_replace(df.title, r'\(\d+\)', '')).alias('title'),
        substring(regexp_extract(df.title, r'\(\d\d\d\d\)', 0), 2, 4).alias('year').cast(IntegerType()),
        split(trim(df.genres), pattern='\|').alias('genres')
    ).filter(col('year').isNotNull())

df2 \
    .write \
    .format('org.apache.spark.sql.cassandra') \
    .options(table='movies', keyspace=keyspace) \
    .mode('append') \
    .save()

sc.stop()

