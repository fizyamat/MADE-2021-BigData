#! /usr/bin/env pyhton3

import re
from pyspark import SparkContext


PATH_TO_DATASET = 'hdfs:///data/wiki/en_articles_part/'

sc = SparkContext()

wiki_rdd = sc.textFile(PATH_TO_DATASET)

words_rdd = (
    wiki_rdd
    .map(lambda x: x.split('\t', 1))
    .map(lambda x: x[1].lower())
    .map(lambda x: re.findall(r"\w+", x))
    .map(lambda x: [f'{x[i-1]}_{x[i]}' for i in range(1, len(x)) if x[i-1] == 'narodnaya'])
    .filter(lambda x: len(x) > 0)
    .flatMap(lambda x: x)
    .map(lambda x: (x, 1))
    .reduceByKey(lambda x, y: x + y)
    .takeOrdered(10, lambda x: x[0])
)

for word, count in words_rdd:
    print(word, count, sep='\t')

sc.stop()
