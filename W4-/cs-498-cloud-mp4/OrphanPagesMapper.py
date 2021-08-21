#!/usr/bin/env python3
import sys

for line in sys.stdin:
    # TODO
    src, des_str = line.strip().split(':', 1)

    src = src.strip()
    des_list = des_str.strip().split(' ')

    des_set = set([it.strip() for it in des_list if it.strip() != ''])

    # remove self linking
    if src in des_set:
        des_set.remove(src)

    print('%s\t%s' % ( src, ','.join(list(des_set)) ))
