#!/usr/bin/env python3
#from operator import itemgetter
import sys

src_set = set()
des_set = set()

# input comes from STDIN
for line in sys.stdin:
    line = line.strip()

    # parse input from mappers
    src, des_str = line.split('\t', 1)

    # convert count (currently a string) to int
    try:
        des_list = des_str.strip().split(',')
        des_new_set = set([it.strip() for it in des_list if it.strip() != ''])
    except ValueError:
        # count was not a bool, so silently
        # ignore/discard this line
        continue

    src_set.add(src)
    des_set = des_set.union(des_new_set)

# do not forget to output the last page ID if needed!
res = []
for src in src_set:
    if src not in des_set:
        res.append(int(src))
for src in sorted(res):
    print(src)
