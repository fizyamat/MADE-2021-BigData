#! /usr/bin/env python3

import sys
import re

from lxml import etree

YEARS = set([2016, 2010])
SEPARATOR = '\t'

data = sys.stdin.readlines()

for line in data:

    line = line.strip()
    if not line.startswith('<row'):
        continue

    root = etree.fromstring(line)
    year = int(root.attrib.get('CreationDate')[:4])
    if year not in YEARS:
        continue

    tags = root.attrib.get('Tags')
    if tags:
        tags_list = re.split('<|><|>', tags)[1:-1]

        for tag in tags_list:
            print(year, tag, 1, sep=SEPARATOR)
