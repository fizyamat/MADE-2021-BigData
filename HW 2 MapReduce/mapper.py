#! /usr/bin/python

import sys

from random import randint

START_INDEX = 1
END_INDEX = 10 ** 5 - 1

data = sys.stdin.readlines()

for line in data:
    prefix = randint(START_INDEX, END_INDEX)
    print(f'{prefix}_{line}')
