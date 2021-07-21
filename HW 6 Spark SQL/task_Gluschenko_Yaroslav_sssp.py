# !/usr/bin/env python3

from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col


PATH_TO_DATA = 'hdfs:///data/twitter/twitter.txt'

# create spark context
sc = SparkContext()
spark = SparkSession(sc)

# read dataset to spark
df = spark.read.format('csv').load(PATH_TO_DATA, sep='\t')

right = (
    df.withColumnRenamed('_c0', 'id_right')
    .withColumnRenamed('_c1', 'follower')
)

left = (
    right.filter(col('id_right') == 34)
    .withColumnRenamed('id_right', 'id')
)

# body
length = 1
while True:

    left = left.withColumnRenamed('follower', 'follower_left')

    join_condition = (col('follower_left') == col('id_right'))

    left = (
        left.join(right, join_condition, 'inner')
        .drop('follower_left')
        .drop('id_right')
    )

    length += 1

    if left.filter(col('follower') == 12).count():
        break


# Result
print(length)

sc.stop()
