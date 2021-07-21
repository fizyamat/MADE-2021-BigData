#! /usr/bin/env python3

import sys

data = sys.stdin.readlines()

YEARS = [2010, 2016, None]

i = 0
current_year = YEARS[0]
num = 0

for line in data:
    line = line.strip()
    year, count, word = line.split('\t')
    year = int(year)
    if current_year == year:
        print(year, word, count, sep='\t')
        num += 1

    if num == 10:
        num = 0
        i += 1
        current_year = YEARS[i]
