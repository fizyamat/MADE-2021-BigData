#! /usr/bin/env/python3

import re

from math import log
from pyspark import SparkContext


PATH_TO_DATASET = 'hdfs:///data/wiki/en_articles_part/'
STOPWORDS = 'hdfs:///data/stop_words/stop_words_en-xpo6.txt'

sc = SparkContext()

wiki_rdd = sc.textFile(PATH_TO_DATASET)
stopwords_rdd = sc.textFile(STOPWORDS)

stopwords_broadcast = sc.broadcast(
    set(stopwords_rdd.collect())
)

words_rdd = (
    wiki_rdd
    .map(lambda x: x.split('\t', 1))
    .map(lambda x: x[1].lower())
    .map(lambda x: re.findall(r"\w+", x))
    .map(lambda x: [i for i in x if i not in stopwords_broadcast.value])
)

all_words_rdd = (
    words_rdd
    .flatMap(lambda x: [(i, 1) for i in x])
)

total_count_words = all_words_rdd.count()
total_count_pairs = total_count_words + wiki_rdd.count()

all_words_broadcast = sc.broadcast(
    {key: value for key, value in all_words_rdd
     .reduceByKey(lambda x, y: x + y)
     .collect()
    }
)

def NPMI(ab, count):
    a, _, b = ab.partition('_')
    p_a = all_words_broadcast.value.get(a) / total_count_words
    p_b = all_words_broadcast.value.get(b) / total_count_words

    if p_a * p_b == 0:
        return 0

    p_ab = count / total_count_pairs
    PMI = log(p_ab / (p_a * p_b))

    return round(PMI / (-log(p_ab)), 3)

result = (
    words_rdd
    .map(lambda x: [(f'{x[i-1]}_{x[i]}', 1) for i in range(1, len(x))])
    .flatMap(lambda x: x)
    .reduceByKey(lambda x, y: x + y)
    .filter(lambda x: x[1] >= 500)
    .map(lambda x: (x[0], NPMI(x[0], x[1])))
    .takeOrdered(39, lambda x: -x[1])
)

for pair, npmi in result:
    print(pair, npmi, sep='\t')

sc.stop()
