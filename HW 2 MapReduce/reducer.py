#! /usr/bin/python

import sys

data = sys.stdin.readlines()

length = 0
string = []
for line in data:
    line = line.strip()
    if length == 0:
        length = len(line)
        string.append(line.rpartition("_")[2])

    elif length != len(line) or len(string) == 5:
        print(','.join(string), sep = '')

        string.clear()
        length = len(line)
        string.append(line.rpartition("_")[2])

    else:
        string.append(line.rpartition("_")[2])
