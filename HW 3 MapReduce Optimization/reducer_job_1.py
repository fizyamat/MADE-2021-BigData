#! /usr/bin/env python3

import sys

data = sys.stdin.readlines()

current_tag = None
current_year = None
first_line = True
current_count = 0

for line in data:

    year, tag, count = line.split('\t')
    count = int(count)

    if first_line:
        first_line = False
        current_year = year
        current_tag = tag
        current_count = count

    else:
        if current_year == year and current_tag == tag:
            current_count += count

        else:
            print(current_year, current_tag, current_count, sep='\t')
            current_year = year
            current_tag = tag
            current_count = count

print(current_year, current_tag, current_count, sep='\t')
