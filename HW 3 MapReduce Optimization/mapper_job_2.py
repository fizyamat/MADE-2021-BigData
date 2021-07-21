#! /usr/bin/env python3

import sys

data = sys.stdin.readlines()

for line in data:

    year, word, count = line.split('\t')
    year = int(year)
    count = int(count)

    print(year, count, word, sep='\t')
