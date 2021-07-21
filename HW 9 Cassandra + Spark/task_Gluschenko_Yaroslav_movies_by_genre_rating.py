# /usr/bin/env python3

from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import split, trim, col, regexp_replace, regexp_extract, substring, explode
from pyspark.sql.types import IntegerType

import sys

PATH_TO_MOVIE_DATA = 'hdfs:///data/movielens/movies.csv'
PATH_TO_RATING_DATA = 'hdfs:///data/movielens/ratings.csv'
SEPARATOR = ','


keyspace = sys.argv[1]

# create spark context
sc = SparkContext()
spark = SparkSession(sc)

# read dataset to spark
df_movie = spark \
    .read \
    .format('csv') \
    .load(PATH_TO_MOVIE_DATA, sep=SEPARATOR, header=True)

df_rating = spark \
    .read \
    .format('csv') \
    .load(PATH_TO_RATING_DATA, sep=SEPARATOR, header=True)

df = df_movie.join(df_rating, ["movieId"])

df2 = df \
    .filter(col('genres') != '(no genres listed)') \
    .select(
        df.movieId.cast(IntegerType()).alias('movieid'),
        trim(regexp_replace(df.title, r'\(\d+\)', '')).alias('title'),
 #       df.userId.cast(IntegerType()).alias('userid'),
        df.rating,
        df.timestamp,
        substring(regexp_extract(df.title, r'\(\d\d\d\d\)', 0), 2, 4).alias('year').cast(IntegerType()),
        split(trim(df.genres), pattern='\|').alias('genres')
    ).filter(col('year').isNotNull()) \
    .orderBy(col('year').desc())

df3 = df2.select(
    df2.movieid,
    df2.title,
#    df2.userid,
    df2.rating,
    df2.timestamp,
    df2.year,
    explode(df2.genres).alias('genre')
)

df3 \
    .write \
    .format('org.apache.spark.sql.cassandra') \
    .options(table='movies_by_genre_rating', keyspace=keyspace) \
    .mode('append') \
    .save()

sc.stop()
